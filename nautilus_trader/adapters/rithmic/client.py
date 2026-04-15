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
Thin wrapper around :class:`async_rithmic.RithmicClient` that:

* Manages ORDER + PNL plant connections.
* Routes notification callbacks to registered handlers on the execution client.
* Caches fill events locally so they can be replayed for reconciliation.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from async_rithmic import ReconnectionSettings
from async_rithmic import RithmicClient
from async_rithmic import RetrySettings

from nautilus_trader.common.component import Logger


@dataclass
class CachedFill:
    """A fill event cached for later reconciliation via ``generate_fill_reports``."""

    basket_id: str
    fill_id: str
    symbol: str
    exchange: str
    account_id: str
    transaction_type: int   # Rithmic TransactionType int value
    fill_price: float
    fill_size: int
    ssboe: int
    usecs: int


@dataclass
class CachedOrderState:
    """Latest known order state, updated on every Rithmic/exchange notification."""

    basket_id: str
    symbol: str
    exchange: str
    account_id: str
    transaction_type: int
    price_type: int
    duration: int
    quantity: int
    filled_qty: int
    avg_fill_price: float
    price: float
    trigger_price: float
    rithmic_notify_type: int  # latest Rithmic notify_type seen
    exchange_notify_type: int  # latest exchange notify_type seen
    ssboe: int
    usecs: int


class RithmicClientWrapper:
    """
    Wraps :class:`async_rithmic.RithmicClient` for the NautilusTrader execution adapter.

    Parameters
    ----------
    user : str
        Rithmic login username.
    password : str
        Rithmic login password.
    system_name : str
        Rithmic system/server name.
    app_name : str
        Registered application name.
    app_version : str
        Application version string.
    url : str
        Rithmic gateway WebSocket URL.
    logger : Logger
        NautilusTrader logger instance.
    """

    def __init__(
        self,
        user: str,
        password: str,
        system_name: str,
        app_name: str,
        app_version: str,
        url: str,
        logger: Logger,
    ) -> None:
        self._log = logger
        self._client = RithmicClient(
            user=user,
            password=password,
            system_name=system_name,
            app_name=app_name,
            app_version=app_version,
            url=url,
            reconnection_settings=ReconnectionSettings(
                backoff_type="linear",
                interval=10,
                max_delay=120,
            ),
            retry_settings=RetrySettings(
                max_retries=3,
                timeout=30.0,
            ),
        )

        # Callback slots — set by the execution client after construction.
        self.on_rithmic_order_notification: Callable[[Any], None] | None = None
        self.on_exchange_order_notification: Callable[[Any], None] | None = None
        self.on_account_pnl_update: Callable[[Any], None] | None = None
        self.on_instrument_pnl_update: Callable[[Any], None] | None = None

        # Local caches for reconciliation.
        self._fills: dict[str, CachedFill] = {}             # fill_id → CachedFill
        self._order_states: dict[str, CachedOrderState] = {}  # basket_id → CachedOrderState

        self._connected = asyncio.Event()

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Connect ORDER and PNL plants and register callbacks."""
        self._log.info("Connecting Rithmic ORDER and PNL plants")

        # Register callbacks before connecting so nothing is missed.
        self._client.on_rithmic_order_notification += self._handle_rithmic_order_notification
        self._client.on_exchange_order_notification += self._handle_exchange_order_notification
        self._client.on_account_pnl_update += self._handle_account_pnl_update
        self._client.on_instrument_pnl_update += self._handle_instrument_pnl_update
        self._client.on_connected += self._handle_connected

        await self._client.connect(ORDER=True, PNL=True)

    async def disconnect(self) -> None:
        """Disconnect all plants gracefully."""
        self._log.info("Disconnecting Rithmic plants")
        await self._client.disconnect()
        self._connected.clear()

    async def wait_connected(self, timeout: float = 30.0) -> None:
        """Block until the ORDER plant has completed login."""
        await asyncio.wait_for(self._connected.wait(), timeout=timeout)

    # ------------------------------------------------------------------
    # Internal callback handlers
    # ------------------------------------------------------------------

    async def _handle_connected(self, plant_type: Any) -> None:
        self._log.info(f"Rithmic plant connected: {plant_type}")
        self._connected.set()

    async def _handle_rithmic_order_notification(self, notification: Any) -> None:
        basket_id: str = getattr(notification, "basket_id", "")
        if basket_id:
            self._update_order_state_from_rithmic(notification)
        if self.on_rithmic_order_notification is not None:
            await self.on_rithmic_order_notification(notification)

    async def _handle_exchange_order_notification(self, notification: Any) -> None:
        basket_id: str = getattr(notification, "basket_id", "")
        notify_type: int = getattr(notification, "notify_type", 0)
        if basket_id:
            self._update_order_state_from_exchange(notification)
            # Cache fills for reconciliation.
            from async_rithmic import ExchangeOrderNotificationType  # local import avoids circular
            if notify_type == ExchangeOrderNotificationType.FILL:
                self._cache_fill(notification)
        if self.on_exchange_order_notification is not None:
            await self.on_exchange_order_notification(notification)

    async def _handle_account_pnl_update(self, update: Any) -> None:
        if self.on_account_pnl_update is not None:
            await self.on_account_pnl_update(update)

    async def _handle_instrument_pnl_update(self, update: Any) -> None:
        if self.on_instrument_pnl_update is not None:
            await self.on_instrument_pnl_update(update)

    # ------------------------------------------------------------------
    # Order / fill caching
    # ------------------------------------------------------------------

    def _update_order_state_from_rithmic(self, n: Any) -> None:
        basket_id: str = n.basket_id
        existing = self._order_states.get(basket_id)
        if existing is None:
            self._order_states[basket_id] = CachedOrderState(
                basket_id=basket_id,
                symbol=getattr(n, "symbol", ""),
                exchange=getattr(n, "exchange", ""),
                account_id=getattr(n, "account_id", ""),
                transaction_type=getattr(n, "transaction_type", 0),
                price_type=getattr(n, "price_type", 0),
                duration=getattr(n, "duration", 0),
                quantity=getattr(n, "quantity", 0),
                filled_qty=getattr(n, "total_fill_size", 0),
                avg_fill_price=getattr(n, "avg_fill_price", 0.0),
                price=getattr(n, "price", 0.0),
                trigger_price=getattr(n, "trigger_price", 0.0),
                rithmic_notify_type=getattr(n, "notify_type", 0),
                exchange_notify_type=0,
                ssboe=getattr(n, "ssboe", 0),
                usecs=getattr(n, "usecs", 0),
            )
        else:
            existing.rithmic_notify_type = getattr(n, "notify_type", existing.rithmic_notify_type)
            existing.filled_qty = getattr(n, "total_fill_size", existing.filled_qty)
            existing.avg_fill_price = getattr(n, "avg_fill_price", existing.avg_fill_price)
            existing.ssboe = getattr(n, "ssboe", existing.ssboe)
            existing.usecs = getattr(n, "usecs", existing.usecs)

    def _update_order_state_from_exchange(self, n: Any) -> None:
        basket_id: str = n.basket_id
        existing = self._order_states.get(basket_id)
        if existing is None:
            self._order_states[basket_id] = CachedOrderState(
                basket_id=basket_id,
                symbol=getattr(n, "symbol", ""),
                exchange=getattr(n, "exchange", ""),
                account_id=getattr(n, "account_id", ""),
                transaction_type=getattr(n, "transaction_type", 0),
                price_type=getattr(n, "price_type", 0),
                duration=getattr(n, "duration", 0),
                quantity=getattr(n, "quantity", 0),
                filled_qty=getattr(n, "total_fill_size", 0),
                avg_fill_price=getattr(n, "avg_fill_price", 0.0),
                price=getattr(n, "price", 0.0),
                trigger_price=getattr(n, "trigger_price", 0.0),
                rithmic_notify_type=0,
                exchange_notify_type=getattr(n, "notify_type", 0),
                ssboe=getattr(n, "ssboe", 0),
                usecs=getattr(n, "usecs", 0),
            )
        else:
            existing.exchange_notify_type = getattr(n, "notify_type", existing.exchange_notify_type)
            existing.filled_qty = getattr(n, "total_fill_size", existing.filled_qty)
            existing.avg_fill_price = getattr(n, "avg_fill_price", existing.avg_fill_price)
            existing.ssboe = getattr(n, "ssboe", existing.ssboe)
            existing.usecs = getattr(n, "usecs", existing.usecs)

    def _cache_fill(self, n: Any) -> None:
        fill_id: str = getattr(n, "fill_id", "")
        if not fill_id:
            return
        if fill_id in self._fills:
            return  # idempotent
        self._fills[fill_id] = CachedFill(
            basket_id=getattr(n, "basket_id", ""),
            fill_id=fill_id,
            symbol=getattr(n, "symbol", ""),
            exchange=getattr(n, "exchange", ""),
            account_id=getattr(n, "account_id", ""),
            transaction_type=getattr(n, "transaction_type", 0),
            fill_price=getattr(n, "fill_price", 0.0),
            fill_size=getattr(n, "fill_size", 0),
            ssboe=getattr(n, "ssboe", 0),
            usecs=getattr(n, "usecs", 0),
        )

    # ------------------------------------------------------------------
    # Public accessors
    # ------------------------------------------------------------------

    @property
    def fills(self) -> dict[str, CachedFill]:
        """All cached fills keyed by ``fill_id``."""
        return self._fills

    @property
    def order_states(self) -> dict[str, CachedOrderState]:
        """Latest order states keyed by ``basket_id``."""
        return self._order_states

    @property
    def accounts(self) -> list[Any]:
        """Return the list of accounts from the ORDER plant."""
        return self._client.accounts

    # ------------------------------------------------------------------
    # Order operations — thin pass-through to the ORDER plant
    # ------------------------------------------------------------------

    async def submit_order(self, **kwargs: Any) -> Any:
        return await self._client.plants["order"].submit_order(**kwargs)

    async def cancel_order(self, **kwargs: Any) -> Any:
        return await self._client.plants["order"].cancel_order(**kwargs)

    async def cancel_all_orders(self, **kwargs: Any) -> Any:
        return await self._client.plants["order"].cancel_all_orders(**kwargs)

    async def modify_order(self, **kwargs: Any) -> Any:
        return await self._client.plants["order"].modify_order(**kwargs)

    async def list_orders(self, **kwargs: Any) -> list[Any]:
        return await self._client.plants["order"].list_orders(**kwargs)

    async def get_order(self, **kwargs: Any) -> Any:
        return await self._client.plants["order"].get_order(**kwargs)

    # ------------------------------------------------------------------
    # PNL plant operations
    # ------------------------------------------------------------------

    async def list_positions(self, **kwargs: Any) -> list[Any]:
        """Return instrument PnL/position snapshots from the PNL plant."""
        return await self._client.plants["pnl"].list_positions(**kwargs)

    async def list_account_summary(self, **kwargs: Any) -> list[Any]:
        """Return account PnL/balance snapshots from the PNL plant."""
        return await self._client.plants["pnl"].list_account_summary(**kwargs)

    async def subscribe_to_pnl_updates(self) -> None:
        await self._client.plants["pnl"].subscribe_to_pnl_updates()

    # ------------------------------------------------------------------
    # Order history
    # ------------------------------------------------------------------

    async def show_order_history_dates(self) -> list[str]:
        """Return available order history dates as ``"YYYYMMDD"`` strings."""
        results = await self._client.plants["order"].show_order_history_dates()
        dates: list[str] = []
        for item in results:
            for d in getattr(item, "date", []):
                if d:
                    dates.append(d)
        return dates

    async def show_order_history_summary(self, date: str) -> list[Any]:
        """
        Return exchange order notification snapshots for the given date.

        ``date`` must be a ``"YYYYMMDD"`` string.  The returned objects are
        template-352 ``ExchangeOrderNotification`` messages with
        ``is_snapshot=True``.
        """
        return await self._client.plants["order"].show_order_history_summary(date=date)

    def ingest_historical_notification(self, n: Any) -> None:
        """
        Process a historical exchange order notification into the fill cache.

        Call this for each item returned by :meth:`show_order_history_summary`
        so that ``generate_fill_reports`` can include past-session fills.
        """
        from async_rithmic import ExchangeOrderNotificationType
        basket_id: str = getattr(n, "basket_id", "")
        notify_type: int = getattr(n, "notify_type", 0)
        if basket_id:
            self._update_order_state_from_exchange(n)
        if notify_type == ExchangeOrderNotificationType.FILL:
            self._cache_fill(n)
