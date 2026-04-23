# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

"""
Live execution client for the Rithmic adapter.

Design notes
------------
* ``venue=None`` — Rithmic is a multi-venue intermediary (same as IB adapter).
  The actual venue is derived from the exchange field on each instrument.
* ``OmsType.NETTING`` — futures positions are always netted.
* ``AccountType.MARGIN`` — futures require margin.
* A single :class:`RithmicClientWrapper` per execution client.
* Fills are streamed via ``on_exchange_order_notification`` callbacks and cached
  in the client wrapper.  On reconciliation (``generate_fill_reports``) the
  cached dict is replayed so the framework can reconcile state.
* Account state is driven by PNL plant ``on_account_pnl_update`` callbacks and
  also queried once on connect via ``list_account_summary``.
"""

from __future__ import annotations

import asyncio
import datetime
from decimal import Decimal
from typing import Any

from async_rithmic import ExchangeOrderNotificationType

from nautilus_trader.adapters.rithmic.client import RithmicClientWrapper
from nautilus_trader.adapters.rithmic.config import RithmicExecClientConfig
from nautilus_trader.adapters.rithmic.parsing.execution import DEFAULT_LIQUIDITY_SIDE
from nautilus_trader.adapters.rithmic.parsing.execution import EXCHANGE_NOTIFY_TO_ORDER_STATUS
from nautilus_trader.adapters.rithmic.parsing.execution import RITHMIC_DURATION_TO_NT
from nautilus_trader.adapters.rithmic.parsing.execution import RITHMIC_NOTIFY_TO_ORDER_STATUS
from nautilus_trader.adapters.rithmic.parsing.execution import RITHMIC_ORDER_TYPE_TO_NT
from nautilus_trader.adapters.rithmic.parsing.execution import RITHMIC_TRANSACTION_TYPE_TO_NT
from nautilus_trader.adapters.rithmic.parsing.execution import NT_ORDER_SIDE_TO_RITHMIC
from nautilus_trader.adapters.rithmic.parsing.execution import NT_ORDER_TYPE_TO_RITHMIC
from nautilus_trader.adapters.rithmic.parsing.execution import NT_TIME_IN_FORCE_TO_RITHMIC
from nautilus_trader.adapters.rithmic.parsing.execution import ssboe_usecs_to_nanos
from nautilus_trader.adapters.rithmic.parsing.instruments import RITHMIC_EXCHANGE_TO_MIC
from nautilus_trader.adapters.rithmic.parsing.instruments import make_instrument_id
from nautilus_trader.adapters.rithmic.providers import RithmicInstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import BatchCancelOrders
from nautilus_trader.execution.messages import CancelAllOrders
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.execution.messages import SubmitOrderList
from nautilus_trader.execution.reports import ExecutionMassStatus
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import ContingencyType
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import MarginBalance
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders.base import Order


class RithmicLiveExecutionClient(LiveExecutionClient):
    """
    Live execution client for Rithmic via the ``async_rithmic`` library.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    client : RithmicClientWrapper
        The Rithmic client wrapper.
    account_id : AccountId
        The trading account ID.
    msgbus : MessageBus
        The NautilusTrader message bus.
    cache : Cache
        The NautilusTrader cache.
    clock : LiveClock
        The clock.
    instrument_provider : RithmicInstrumentProvider
        The instrument provider.
    config : RithmicExecClientConfig
        Adapter configuration.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client: RithmicClientWrapper,
        account_id: AccountId,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: RithmicInstrumentProvider,
        config: RithmicExecClientConfig,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(account_id.get_issuer()),
            venue=None,  # Multi-venue intermediary — same pattern as IB adapter
            oms_type=OmsType.NETTING,
            instrument_provider=instrument_provider,
            account_type=AccountType.MARGIN,
            base_currency=None,  # Multi-currency account
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=config,
        )

        self._client = client
        self._account_id = account_id
        self._config = config

        # Register our callbacks on the wrapper.
        self._client.on_rithmic_order_notification = self._on_rithmic_order_notification
        self._client.on_exchange_order_notification = self._on_exchange_order_notification
        self._client.on_account_pnl_update = self._on_account_pnl_update
        self._client.on_instrument_pnl_update = self._on_instrument_pnl_update

        # Map client_order_id → basket_id for cancel/modify routing.
        self._client_order_id_to_basket: dict[str, str] = {}
        # Map basket_id → client_order_id for event routing back to strategies.
        self._basket_to_client_order_id: dict[str, str] = {}

    # ------------------------------------------------------------------
    # LiveExecutionClient lifecycle
    # ------------------------------------------------------------------

    async def _connect(self) -> None:
        self._log.info("Connecting to Rithmic…")
        await self._client.connect()
        await self._client.wait_connected(timeout=self._config.connection_timeout)

        # Subscribe to real-time PNL/account updates.
        await self._client.subscribe_to_pnl_updates()

        # Load instruments from provider.
        await self._instrument_provider.load_all_async()

        # Register account_id with the base class before emitting account state.
        if self.account_id is None:
            self._set_account_id(self._account_id)

        # Prime account state with a one-shot snapshot.
        await self._query_account_snapshot()

        # Back-fill historical fills if configured.
        if self._config.fill_history_days > 0:
            await self._load_fill_history(self._config.fill_history_days)

        self._log.info("Rithmic execution client connected.")

    async def _disconnect(self) -> None:
        self._log.info("Disconnecting from Rithmic…")
        await self._client.disconnect()

    # ------------------------------------------------------------------
    # Order commands
    # ------------------------------------------------------------------

    async def _submit_order(self, command: SubmitOrder) -> None:
        order: Order = command.order
        instrument_id: InstrumentId = order.instrument_id

        rithmic_order_type = NT_ORDER_TYPE_TO_RITHMIC.get(order.order_type)
        if rithmic_order_type is None:
            self._log.error(f"Unsupported order type: {order.order_type}")
            self.generate_order_rejected(
                strategy_id=order.strategy_id,
                instrument_id=instrument_id,
                client_order_id=order.client_order_id,
                reason=f"Unsupported order type: {order.order_type}",
                ts_event=self._clock.timestamp_ns(),
            )
            return

        rithmic_side = NT_ORDER_SIDE_TO_RITHMIC.get(order.side)
        rithmic_duration = NT_TIME_IN_FORCE_TO_RITHMIC.get(order.time_in_force, None)

        kwargs: dict[str, Any] = {
            "order_id": order.client_order_id.value,
            "symbol": instrument_id.symbol.value,
            "exchange": self._mic_to_rithmic_exchange(instrument_id),
            "qty": int(order.quantity),
            "transaction_type": rithmic_side,
            "order_type": rithmic_order_type,
        }

        if rithmic_duration is not None:
            kwargs["duration"] = rithmic_duration

        if order.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT, OrderType.LIMIT_IF_TOUCHED):
            kwargs["price"] = float(order.price)

        if order.order_type in (
            OrderType.STOP_MARKET,
            OrderType.STOP_LIMIT,
            OrderType.MARKET_IF_TOUCHED,
            OrderType.LIMIT_IF_TOUCHED,
        ):
            kwargs["trigger_price"] = float(order.trigger_price)

        kwargs["account_id"] = self._account_id.get_id()

        try:
            response = await self._client.submit_order(**kwargs)
            basket_id: str = getattr(response, "basket_id", order.client_order_id.value)
            self._register_order(order.client_order_id.value, basket_id)
            self.generate_order_submitted(
                strategy_id=order.strategy_id,
                instrument_id=instrument_id,
                client_order_id=order.client_order_id,
                ts_event=self._clock.timestamp_ns(),
            )
        except Exception as e:
            self._log.error(f"submit_order failed: {e}")
            self.generate_order_rejected(
                strategy_id=order.strategy_id,
                instrument_id=instrument_id,
                client_order_id=order.client_order_id,
                reason=str(e),
                ts_event=self._clock.timestamp_ns(),
            )

    async def _submit_order_list(self, command: SubmitOrderList) -> None:
        order_list = command.order_list
        is_bracket = (
            len(order_list.orders) >= 2
            and order_list.orders[0].contingency_type == ContingencyType.OTO
        )
        if is_bracket:
            await self._submit_bracket_order(command)
        else:
            # Fall back to submitting each order individually.
            for order in command.order_list.orders:
                submit = SubmitOrder(
                    trader_id=command.trader_id,
                    strategy_id=command.strategy_id,
                    order=order,
                    command_id=UUID4(),
                    ts_init=self._clock.timestamp_ns(),
                )
                await self._submit_order(submit)

    async def _submit_bracket_order(self, command: SubmitOrderList) -> None:
        """
        Submit a bracket order list using Rithmic's native bracket support.

        The entry order triggers when filled; Rithmic then automatically manages
        the take-profit and stop-loss legs as an OCO pair via ``stop_ticks`` /
        ``target_ticks``.

        For MARKET entry orders the exact fill price is unknown ahead of time so
        we fall back to submitting each leg individually — Rithmic cannot place
        bracket legs relative to an unknown fill price from our side.
        """
        orders = command.order_list.orders
        entry = orders[0]
        children = orders[1:]  # [tp, sl] or [sl, tp] — order may vary

        # Identify take-profit (LIMIT) and stop-loss (STOP_*) child legs.
        tp_order: Order | None = None
        sl_order: Order | None = None
        for child in children:
            if child.order_type in (OrderType.LIMIT, OrderType.LIMIT_IF_TOUCHED):
                tp_order = child
            elif child.order_type in (
                OrderType.STOP_MARKET,
                OrderType.STOP_LIMIT,
                OrderType.MARKET_IF_TOUCHED,
            ):
                sl_order = child

        # MARKET entries: cannot compute ticks without a known entry price.
        if entry.order_type == OrderType.MARKET or (tp_order is None and sl_order is None):
            self._log.warning(
                f"Bracket order {command.order_list.id} has a MARKET entry or unrecognised "
                "child order types — falling back to individual order submission.",
            )
            for order in orders:
                await self._submit_order(
                    SubmitOrder(
                        trader_id=command.trader_id,
                        strategy_id=command.strategy_id,
                        order=order,
                        command_id=UUID4(),
                        ts_init=self._clock.timestamp_ns(),
                    )
                )
            return

        instrument_id = entry.instrument_id
        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            self._log.error(f"Bracket order failed — instrument not found: {instrument_id}")
            return

        tick_size: float = float(instrument.price_increment)
        entry_price: float = float(entry.price)
        is_buy = entry.side == OrderSide.BUY

        # Compute tick distances.  Both values must be positive integers.
        target_ticks: int | None = None
        stop_ticks: int | None = None

        if tp_order is not None:
            tp_price = float(tp_order.price)
            raw = (tp_price - entry_price) if is_buy else (entry_price - tp_price)
            target_ticks = max(1, round(raw / tick_size))

        if sl_order is not None:
            sl_price = float(
                sl_order.trigger_price
                if hasattr(sl_order, "trigger_price") and sl_order.trigger_price
                else sl_order.price
            )
            raw = (entry_price - sl_price) if is_buy else (sl_price - entry_price)
            stop_ticks = max(1, round(raw / tick_size))

        rithmic_order_type = NT_ORDER_TYPE_TO_RITHMIC.get(entry.order_type)
        rithmic_side = NT_ORDER_SIDE_TO_RITHMIC.get(entry.side)
        rithmic_duration = NT_TIME_IN_FORCE_TO_RITHMIC.get(entry.time_in_force)

        kwargs: dict[str, Any] = {
            "order_id": entry.client_order_id.value,
            "symbol": instrument_id.symbol.value,
            "exchange": self._mic_to_rithmic_exchange(instrument_id),
            "qty": int(entry.quantity),
            "transaction_type": rithmic_side,
            "order_type": rithmic_order_type,
            "account_id": self._account_id.get_id(),
        }
        if rithmic_duration is not None:
            kwargs["duration"] = rithmic_duration
        if entry.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT):
            kwargs["price"] = entry_price
        if entry.order_type in (OrderType.STOP_MARKET, OrderType.STOP_LIMIT):
            kwargs["trigger_price"] = float(entry.trigger_price)
        if target_ticks is not None:
            kwargs["target_ticks"] = target_ticks
        if stop_ticks is not None:
            kwargs["stop_ticks"] = stop_ticks

        try:
            response = await self._client.submit_order(**kwargs)
            basket_id: str = getattr(response, "basket_id", entry.client_order_id.value)
            self._register_order(entry.client_order_id.value, basket_id)

            # Register child orders too — Rithmic creates them internally but we
            # still need them in the cache for event routing if they report back.
            for child in children:
                self._register_order(child.client_order_id.value, basket_id)

            self.generate_order_submitted(
                strategy_id=command.strategy_id,
                instrument_id=instrument_id,
                client_order_id=entry.client_order_id,
                ts_event=self._clock.timestamp_ns(),
            )
            for child in children:
                self.generate_order_submitted(
                    strategy_id=command.strategy_id,
                    instrument_id=instrument_id,
                    client_order_id=child.client_order_id,
                    ts_event=self._clock.timestamp_ns(),
                )
        except Exception as e:
            self._log.error(f"bracket submit_order failed: {e}")
            for order in orders:
                self.generate_order_rejected(
                    strategy_id=command.strategy_id,
                    instrument_id=instrument_id,
                    client_order_id=order.client_order_id,
                    reason=str(e),
                    ts_event=self._clock.timestamp_ns(),
                )

    async def _modify_order(self, command: ModifyOrder) -> None:
        basket_id = self._resolve_basket_id(command)
        if basket_id is None:
            return

        kwargs: dict[str, Any] = {
            "basket_id": basket_id,
            "account_id": self._account_id.get_id(),
        }
        if command.quantity is not None:
            kwargs["qty"] = int(command.quantity)
        if command.price is not None:
            kwargs["price"] = float(command.price)
        if command.trigger_price is not None:
            kwargs["trigger_price"] = float(command.trigger_price)

        try:
            await self._client.modify_order(**kwargs)
        except Exception as e:
            self._log.error(f"modify_order failed: {e}")

    async def _cancel_order(self, command: CancelOrder) -> None:
        basket_id = self._resolve_basket_id(command)
        if basket_id is None:
            return
        try:
            await self._client.cancel_order(
                basket_id=basket_id,
                account_id=self._account_id.get_id(),
            )
        except Exception as e:
            self._log.error(f"cancel_order failed: {e}")

    async def _cancel_all_orders(self, command: CancelAllOrders) -> None:
        try:
            await self._client.cancel_all_orders(
                account_id=self._account_id.get_id(),
            )
        except Exception as e:
            self._log.error(f"cancel_all_orders failed: {e}")

    async def _batch_cancel_orders(self, command: BatchCancelOrders) -> None:
        for cancel in command.cancels:
            await self._cancel_order(cancel)

    async def _query_account(self) -> None:
        await self._query_account_snapshot()

    # ------------------------------------------------------------------
    # Reconciliation reports
    # ------------------------------------------------------------------

    async def generate_order_status_report(
        self,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId | None = None,
        venue_order_id: VenueOrderId | None = None,
    ) -> OrderStatusReport | None:

        # Try the local cache first.
        basket_id: str | None = None
        if venue_order_id:
            basket_id = venue_order_id.value
        elif client_order_id:
            basket_id = self._client_order_id_to_basket.get(client_order_id.value)

        if basket_id and basket_id in self._client.order_states:
            return self._order_state_to_report(basket_id)

        # Fall back to a live query.
        if basket_id:
            try:
                raw = await self._client.get_order(
                    basket_id=basket_id,
                    account_id=self._account_id.get_id(),
                )
                if raw:
                    return self._raw_order_to_report(raw, client_order_id)
            except Exception as e:
                self._log.error(f"get_order failed: {e}")
        return None

    async def generate_order_status_reports(
        self,
        instrument_id: InstrumentId | None = None,
        start: Any = None,
        end: Any = None,
        open_only: bool = False,
    ) -> list[OrderStatusReport]:
        reports: list[OrderStatusReport] = []
        try:
            raw_orders = await self._client.list_orders(
                account_id=self._account_id.get_id(),
            )
            for raw in raw_orders:
                basket_id: str = getattr(raw, "basket_id", "")
                if not basket_id:
                    continue
                coid_str = self._basket_to_client_order_id.get(basket_id)
                coid = ClientOrderId(coid_str) if coid_str else None
                report = self._raw_order_to_report(raw, coid)
                if report:
                    reports.append(report)
        except Exception as e:
            self._log.error(f"list_orders failed: {e}")
        return reports

    async def generate_fill_reports(
        self,
        instrument_id: InstrumentId | None = None,
        venue_order_id: VenueOrderId | None = None,
        start: Any = None,
        end: Any = None,
    ) -> list[FillReport]:
        """Replay cached fills collected via ``on_exchange_order_notification``."""
        reports: list[FillReport] = []
        for cached in self._client.fills.values():
            instrument_id = make_instrument_id(cached.symbol, cached.exchange)
            instrument = self._cache.instrument(instrument_id)
            if instrument is None:
                self._log.warning(f"Instrument not found for fill: {instrument_id}")
                continue

            order_side = RITHMIC_TRANSACTION_TYPE_TO_NT.get(cached.transaction_type, OrderSide.BUY)
            coid_str = self._basket_to_client_order_id.get(cached.basket_id)
            coid = ClientOrderId(coid_str) if coid_str else None

            report = FillReport(
                account_id=self._account_id,
                instrument_id=instrument_id,
                venue_order_id=VenueOrderId(cached.basket_id),
                trade_id=TradeId(cached.fill_id),
                order_side=order_side,
                last_qty=Quantity(cached.fill_size, instrument.size_precision),
                last_px=Price(cached.fill_price, instrument.price_precision),
                commission=Money(0, Currency.from_str("USD")),  # Rithmic does not report commission on fills
                liquidity_side=DEFAULT_LIQUIDITY_SIDE,
                report_id=UUID4(),
                ts_event=ssboe_usecs_to_nanos(cached.ssboe, cached.usecs),
                ts_init=self._clock.timestamp_ns(),
                client_order_id=coid,
            )
            reports.append(report)
        return reports

    async def generate_position_status_reports(
        self,
        instrument_id: InstrumentId | None = None,
        start: Any = None,
        end: Any = None,
    ) -> list[PositionStatusReport]:
        reports: list[PositionStatusReport] = []
        try:
            positions = await self._client.list_positions(
                account_id=self._account_id.get_id(),
            )
            for pos in positions:
                symbol: str = getattr(pos, "symbol", "")
                exchange: str = getattr(pos, "exchange", "")
                net_qty: int = getattr(pos, "net_quantity", 0)
                avg_px: float = getattr(pos, "avg_open_fill_price", 0.0)
                ssboe: int = getattr(pos, "ssboe", 0)
                usecs: int = getattr(pos, "usecs", 0)

                if not symbol or not exchange:
                    continue

                instrument_id = make_instrument_id(symbol, exchange)
                instrument = self._cache.instrument(instrument_id)
                if instrument is None:
                    self._log.warning(f"Instrument not found for position: {instrument_id}")
                    continue

                if net_qty > 0:
                    side = PositionSide.LONG
                    qty = Quantity(net_qty, instrument.size_precision)
                elif net_qty < 0:
                    side = PositionSide.SHORT
                    qty = Quantity(abs(net_qty), instrument.size_precision)
                else:
                    side = PositionSide.FLAT
                    qty = Quantity(0, instrument.size_precision)

                report = PositionStatusReport(
                    account_id=self._account_id,
                    instrument_id=instrument_id,
                    position_side=side,
                    quantity=qty,
                    report_id=UUID4(),
                    ts_last=ssboe_usecs_to_nanos(ssboe, usecs),
                    ts_init=self._clock.timestamp_ns(),
                )
                reports.append(report)
        except Exception as e:
            self._log.error(f"list_positions failed: {e}")
        return reports

    async def generate_mass_status(
        self,
        lookback_mins: int | None = None,
    ) -> ExecutionMassStatus | None:
        """
        Generate a full reconciliation report.

        Overrides the base implementation to pass ``venue=None`` since Rithmic
        is a multi-venue intermediary (venue is derived from account_id).
        """
        self._log.info("Generating ExecutionMassStatus for Rithmic…")
        self.reconciliation_active = True
        try:
            now_ns = self._clock.timestamp_ns()

            order_reports, fill_reports, position_reports = await asyncio.gather(
                self.generate_order_status_reports(),
                self.generate_fill_reports(),
                self.generate_position_status_reports(),
            )

            mass_status = ExecutionMassStatus(
                client_id=self.id,
                account_id=self._account_id,
                venue=None,  # multi-venue intermediary
                report_id=UUID4(),
                ts_init=now_ns,
            )
            mass_status.add_order_reports(reports=order_reports)
            mass_status.add_fill_reports(reports=fill_reports)
            mass_status.add_position_reports(reports=position_reports)
            return mass_status
        except Exception as e:
            self._log.exception("Cannot generate ExecutionMassStatus", e)
            return None
        finally:
            self.reconciliation_active = False

    # ------------------------------------------------------------------
    # Streaming event handlers (called by the client wrapper)
    # ------------------------------------------------------------------

    async def _on_rithmic_order_notification(self, n: Any) -> None:
        """Handle Rithmic-side order lifecycle events (template 351)."""
        basket_id: str = getattr(n, "basket_id", "")
        notify_type: int = getattr(n, "notify_type", 0)
        nt_status = RITHMIC_NOTIFY_TO_ORDER_STATUS.get(notify_type)
        if not basket_id or nt_status is None:
            return

        coid_str = self._basket_to_client_order_id.get(basket_id)
        if coid_str is None:
            return  # external order — not submitted by this session

        client_order_id = ClientOrderId(coid_str)
        order = self._cache.order(client_order_id)
        if order is None:
            return

        ts = ssboe_usecs_to_nanos(getattr(n, "ssboe", 0), getattr(n, "usecs", 0))

        if nt_status == OrderStatus.ACCEPTED and order.status not in (
            OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED,
        ):
            self.generate_order_accepted(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=client_order_id,
                venue_order_id=VenueOrderId(basket_id),
                ts_event=ts,
            )
        elif nt_status == OrderStatus.CANCELED:
            self.generate_order_canceled(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=client_order_id,
                venue_order_id=VenueOrderId(basket_id),
                ts_event=ts,
            )
        elif nt_status == OrderStatus.REJECTED:
            reason = getattr(n, "report_text", "") or getattr(n, "text", "") or "Rejected"
            self.generate_order_rejected(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=client_order_id,
                reason=reason,
                ts_event=ts,
            )

    async def _on_exchange_order_notification(self, n: Any) -> None:
        """Handle exchange-level order events including fills (template 352)."""
        basket_id: str = getattr(n, "basket_id", "")
        notify_type: int = getattr(n, "notify_type", 0)
        if not basket_id:
            return

        coid_str = self._basket_to_client_order_id.get(basket_id)
        if coid_str is None:
            return  # external order

        client_order_id = ClientOrderId(coid_str)
        order = self._cache.order(client_order_id)
        if order is None:
            return

        ts = ssboe_usecs_to_nanos(getattr(n, "ssboe", 0), getattr(n, "usecs", 0))

        if notify_type == ExchangeOrderNotificationType.FILL:
            await self._handle_fill(n, order, basket_id, ts)
        elif notify_type == ExchangeOrderNotificationType.CANCEL:
            self.generate_order_canceled(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=client_order_id,
                venue_order_id=VenueOrderId(basket_id),
                ts_event=ts,
            )
        elif notify_type == ExchangeOrderNotificationType.REJECT:
            reason = getattr(n, "report_text", "") or getattr(n, "text", "") or "Rejected by exchange"
            self.generate_order_rejected(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=client_order_id,
                reason=reason,
                ts_event=ts,
            )
        elif notify_type == ExchangeOrderNotificationType.STATUS:
            venue_order_id = VenueOrderId(basket_id)
            if order.status not in (OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED):
                self.generate_order_accepted(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=client_order_id,
                    venue_order_id=venue_order_id,
                    ts_event=ts,
                )

    async def _handle_fill(self, n: Any, order: Order, basket_id: str, ts: int) -> None:
        """Emit a fill event for a FILL exchange notification."""
        fill_size: int = getattr(n, "fill_size", 0)
        fill_price: float = getattr(n, "fill_price", 0.0)
        fill_id: str = getattr(n, "fill_id", "")
        total_fill_size: int = getattr(n, "total_fill_size", 0)
        total_unfilled: int = getattr(n, "total_unfilled_size", 0)

        if not fill_size or not fill_id:
            return

        instrument = self._cache.instrument(order.instrument_id)
        if instrument is None:
            self._log.error(f"Cannot process fill — instrument not found: {order.instrument_id}")
            return

        last_qty = Quantity(fill_size, instrument.size_precision)
        last_px = Price(fill_price, instrument.price_precision)
        commission = Money(0, Currency.from_str("USD"))

        is_last_fill = total_unfilled == 0

        self.generate_order_filled(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            venue_order_id=VenueOrderId(basket_id),
            venue_position_id=None,
            trade_id=TradeId(fill_id),
            order_side=order.side,
            order_type=order.order_type,
            last_qty=last_qty,
            last_px=last_px,
            quote_currency=instrument.quote_currency,
            commission=commission,
            liquidity_side=DEFAULT_LIQUIDITY_SIDE,
            ts_event=ts,
        )

    async def _on_account_pnl_update(self, update: Any) -> None:
        """Handle live account PnL/balance updates from the PNL plant (template 451)."""
        self._emit_account_state_from_pnl(update)

    async def _on_instrument_pnl_update(self, update: Any) -> None:
        """Handle per-instrument PnL updates (template 450) — no action needed for now."""

    # ------------------------------------------------------------------
    # Account state
    # ------------------------------------------------------------------

    async def _query_account_snapshot(self) -> None:
        """Fetch account PnL snapshot and emit account state."""
        try:
            summaries = await self._client.list_account_summary(
                account_id=self._account_id.get_id(),
            )
            for summary in summaries:
                self._emit_account_state_from_pnl(summary)
        except Exception as e:
            self._log.error(f"list_account_summary failed: {e}")

    async def _load_fill_history(self, days: int) -> None:
        """
        Back-fill fills from Rithmic order history for the last ``days`` calendar days.

        Calls ``show_order_history_dates`` to discover available date strings, filters
        to the requested window, then calls ``show_order_history_summary`` for each date.
        Each exchange-notification snapshot with ``notify_type=FILL`` is ingested into
        the client's fill cache so ``generate_fill_reports`` includes past-session fills.
        """
        self._log.info(f"Loading fill history for last {days} day(s)…")
        try:
            available_dates = await self._client.show_order_history_dates()
        except Exception as e:
            self._log.error(f"show_order_history_dates failed: {e}")
            return

        cutoff = datetime.date.today() - datetime.timedelta(days=days)
        cutoff_str = cutoff.strftime("%Y%m%d")

        target_dates = [d for d in available_dates if d >= cutoff_str]
        if not target_dates:
            self._log.info("No historical fill dates found in the requested window.")
            return

        total = 0
        for date_str in sorted(target_dates):
            try:
                notifications = await self._client.show_order_history_summary(date=date_str)
            except Exception as e:
                self._log.error(f"show_order_history_summary({date_str}) failed: {e}")
                continue

            for n in notifications:
                self._client.ingest_historical_notification(n)
                if getattr(n, "notify_type", 0) == ExchangeOrderNotificationType.FILL:
                    total += 1

        self._log.info(f"Fill history loaded: {total} fill(s) across {len(target_dates)} date(s).")

    def _emit_account_state_from_pnl(self, update: Any) -> None:
        """
        Translate an ``AccountPnLPositionUpdate`` (template 451) into a
        NautilusTrader :meth:`generate_account_state` call.
        """
        account_balance_raw: float = float(getattr(update, "account_balance", 0.0) or 0.0)
        cash_on_hand: float = float(getattr(update, "cash_on_hand", 0.0) or 0.0)
        margin_balance: float = float(getattr(update, "margin_balance", 0.0) or 0.0)
        excess_buy_margin: float = float(getattr(update, "excess_buy_margin", 0.0) or 0.0)

        if account_balance_raw == 0.0 and cash_on_hand == 0.0:
            return  # snapshot not yet populated

        currency = Currency.from_str("USD")
        precision = currency.precision
        total_raw = round(account_balance_raw, precision)
        locked_raw = round(max(0.0, account_balance_raw - cash_on_hand), precision)
        free_raw = round(total_raw - locked_raw, precision)  # derived so total - locked == free exactly

        total = Money(total_raw, currency)
        locked = Money(locked_raw, currency)
        free = Money(free_raw, currency)

        balance = AccountBalance(total=total, free=free, locked=locked)

        # Margin (use margin_balance as initial; excess as a proxy for maintenance).
        margin = MarginBalance(
            initial=Money(margin_balance, currency),
            maintenance=Money(max(0.0, margin_balance - excess_buy_margin), currency),
        )

        self.generate_account_state(
            balances=[balance],
            margins=[margin],
            reported=True,
            ts_event=self._clock.timestamp_ns(),
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _register_order(self, client_order_id: str, basket_id: str) -> None:
        self._client_order_id_to_basket[client_order_id] = basket_id
        self._basket_to_client_order_id[basket_id] = client_order_id

    def _resolve_basket_id(self, command: Any) -> str | None:
        """Return the Rithmic basket_id for a cancel/modify command."""
        if hasattr(command, "venue_order_id") and command.venue_order_id:
            return command.venue_order_id.value
        if hasattr(command, "client_order_id") and command.client_order_id:
            return self._client_order_id_to_basket.get(command.client_order_id.value)
        self._log.error(f"Cannot resolve basket_id for command: {command}")
        return None

    @staticmethod
    def _mic_to_rithmic_exchange(instrument_id: InstrumentId) -> str:
        """
        Reverse-map a MIC venue string back to a Rithmic exchange code.

        Builds an inverted lookup from :data:`RITHMIC_EXCHANGE_TO_MIC` on
        first call.  Falls back to the raw venue string if no mapping found.
        """
        if not hasattr(RithmicLiveExecutionClient, "_MIC_TO_RITHMIC"):
            RithmicLiveExecutionClient._MIC_TO_RITHMIC = {
                v: k for k, v in RITHMIC_EXCHANGE_TO_MIC.items()
            }
            # Databento venue aliases that don't appear in the MIC mapping
            RithmicLiveExecutionClient._MIC_TO_RITHMIC.update({
                "GLBX": "CME",   # CME Globex (Databento)
                "XCBT": "CBOT",
                "XNYM": "NYMEX",
                "XCEC": "COMEX",
            })
        venue = instrument_id.venue.value
        return RithmicLiveExecutionClient._MIC_TO_RITHMIC.get(venue, venue)

    def _order_state_to_report(self, basket_id: str) -> OrderStatusReport | None:
        """Build an :class:`OrderStatusReport` from a cached order state."""
        state = self._client.order_states.get(basket_id)
        if state is None:
            return None
        instrument_id = make_instrument_id(state.symbol, state.exchange)
        return self._build_order_status_report(
            basket_id=basket_id,
            instrument_id=instrument_id,
            order_side=RITHMIC_TRANSACTION_TYPE_TO_NT.get(state.transaction_type, OrderSide.BUY),
            order_type=RITHMIC_ORDER_TYPE_TO_NT.get(state.price_type, OrderType.MARKET),
            time_in_force=RITHMIC_DURATION_TO_NT.get(state.duration, TimeInForce.DAY),
            quantity=state.quantity,
            filled_qty=state.filled_qty,
            avg_fill_price=state.avg_fill_price,
            price=state.price,
            trigger_price=state.trigger_price,
            exchange_notify_type=state.exchange_notify_type,
            ssboe=state.ssboe,
            usecs=state.usecs,
        )

    def _raw_order_to_report(
        self,
        raw: Any,
        client_order_id: ClientOrderId | None,
    ) -> OrderStatusReport | None:
        """Build an :class:`OrderStatusReport` from a raw Rithmic order object."""
        basket_id: str = getattr(raw, "basket_id", "")
        symbol: str = getattr(raw, "symbol", "")
        exchange: str = getattr(raw, "exchange", "")
        if not basket_id or not symbol or not exchange:
            return None
        instrument_id = make_instrument_id(symbol, exchange)
        return self._build_order_status_report(
            basket_id=basket_id,
            instrument_id=instrument_id,
            order_side=RITHMIC_TRANSACTION_TYPE_TO_NT.get(
                getattr(raw, "transaction_type", 0), OrderSide.BUY,
            ),
            order_type=RITHMIC_ORDER_TYPE_TO_NT.get(
                getattr(raw, "price_type", 0), OrderType.MARKET,
            ),
            time_in_force=RITHMIC_DURATION_TO_NT.get(
                getattr(raw, "duration", 0), TimeInForce.DAY,
            ),
            quantity=getattr(raw, "quantity", 0),
            filled_qty=getattr(raw, "total_fill_size", 0),
            avg_fill_price=getattr(raw, "avg_fill_price", 0.0),
            price=getattr(raw, "price", 0.0),
            trigger_price=getattr(raw, "trigger_price", 0.0),
            exchange_notify_type=getattr(raw, "notify_type", 0),
            ssboe=getattr(raw, "ssboe", 0),
            usecs=getattr(raw, "usecs", 0),
            client_order_id=client_order_id,
        )

    def _build_order_status_report(  # noqa: PLR0913
        self,
        basket_id: str,
        instrument_id: InstrumentId,
        order_side: OrderSide,
        order_type: OrderType,
        time_in_force: TimeInForce,
        quantity: int,
        filled_qty: int,
        avg_fill_price: float,
        price: float,
        trigger_price: float,
        exchange_notify_type: int,
        ssboe: int,
        usecs: int,
        client_order_id: ClientOrderId | None = None,
    ) -> OrderStatusReport | None:
        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            self._log.warning(f"Instrument not found for order report: {instrument_id}")
            return None

        is_partial = 0 < filled_qty < quantity
        is_complete = filled_qty >= quantity

        if exchange_notify_type == ExchangeOrderNotificationType.REJECT:
            nt_status = OrderStatus.REJECTED
        elif exchange_notify_type == ExchangeOrderNotificationType.CANCEL:
            nt_status = OrderStatus.CANCELED
        elif is_complete:
            nt_status = OrderStatus.FILLED
        elif is_partial:
            nt_status = OrderStatus.PARTIALLY_FILLED
        else:
            nt_status = EXCHANGE_NOTIFY_TO_ORDER_STATUS.get(
                exchange_notify_type, OrderStatus.ACCEPTED,
            )

        ts = ssboe_usecs_to_nanos(ssboe, usecs)

        return OrderStatusReport(
            account_id=self._account_id,
            instrument_id=instrument_id,
            venue_order_id=VenueOrderId(basket_id),
            client_order_id=client_order_id,
            order_side=order_side,
            order_type=order_type,
            time_in_force=time_in_force,
            order_status=nt_status,
            quantity=Quantity(quantity, instrument.size_precision),
            filled_qty=Quantity(filled_qty, instrument.size_precision),
            avg_px=Decimal(str(avg_fill_price)) if avg_fill_price else None,
            price=Price(price, instrument.price_precision) if price else None,
            trigger_price=(
                Price(trigger_price, instrument.price_precision) if trigger_price else None
            ),
            report_id=UUID4(),
            ts_accepted=ts,
            ts_last=ts,
            ts_init=self._clock.timestamp_ns(),
        )
