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
Mapping helpers between Rithmic protocol-buffer enum values and NautilusTrader
model enums.

Rithmic enums are re-exported from ``async_rithmic`` as:
  - ``OrderType``  → ``RequestNewOrder.PriceType``
  - ``OrderDuration`` → ``RequestNewOrder.Duration``
  - ``TransactionType`` → ``RequestNewOrder.TransactionType``
  - ``ExchangeOrderNotificationType`` → ``ExchangeOrderNotification.NotifyType``

The ``RithmicOrderNotification.NotifyType`` values are not re-exported by the
library so they are mapped by integer literal below.
"""

from async_rithmic import ExchangeOrderNotificationType
from async_rithmic import OrderDuration
from async_rithmic import OrderType
from async_rithmic import TransactionType

from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType as NTOrderType
from nautilus_trader.model.enums import TimeInForce


# ---------------------------------------------------------------------------
# NT → Rithmic
# ---------------------------------------------------------------------------

NT_ORDER_TYPE_TO_RITHMIC: dict[NTOrderType, int] = {
    NTOrderType.MARKET: OrderType.MARKET,
    NTOrderType.LIMIT: OrderType.LIMIT,
    NTOrderType.STOP_MARKET: OrderType.STOP_MARKET,
    NTOrderType.STOP_LIMIT: OrderType.STOP_LIMIT,
    NTOrderType.MARKET_IF_TOUCHED: OrderType.MARKET_IF_TOUCHED,
    NTOrderType.LIMIT_IF_TOUCHED: OrderType.LIMIT_IF_TOUCHED,
}

NT_ORDER_SIDE_TO_RITHMIC: dict[OrderSide, int] = {
    OrderSide.BUY: TransactionType.BUY,
    OrderSide.SELL: TransactionType.SELL,
}

NT_TIME_IN_FORCE_TO_RITHMIC: dict[TimeInForce, int] = {
    TimeInForce.DAY: OrderDuration.DAY,
    TimeInForce.GTC: OrderDuration.GTC,
    TimeInForce.IOC: OrderDuration.IOC,
    TimeInForce.FOK: OrderDuration.FOK,
}

# ---------------------------------------------------------------------------
# Rithmic → NT
# ---------------------------------------------------------------------------

RITHMIC_ORDER_TYPE_TO_NT: dict[int, NTOrderType] = {
    OrderType.MARKET: NTOrderType.MARKET,
    OrderType.LIMIT: NTOrderType.LIMIT,
    OrderType.STOP_MARKET: NTOrderType.STOP_MARKET,
    OrderType.STOP_LIMIT: NTOrderType.STOP_LIMIT,
    OrderType.MARKET_IF_TOUCHED: NTOrderType.MARKET_IF_TOUCHED,
    OrderType.LIMIT_IF_TOUCHED: NTOrderType.LIMIT_IF_TOUCHED,
}

RITHMIC_TRANSACTION_TYPE_TO_NT: dict[int, OrderSide] = {
    TransactionType.BUY: OrderSide.BUY,
    TransactionType.SELL: OrderSide.SELL,
    3: OrderSide.SELL,  # SS (short-sell) — value 3 appears in notification protos only
}

RITHMIC_DURATION_TO_NT: dict[int, TimeInForce] = {
    OrderDuration.DAY: TimeInForce.DAY,
    OrderDuration.GTC: TimeInForce.GTC,
    OrderDuration.IOC: TimeInForce.IOC,
    OrderDuration.FOK: TimeInForce.FOK,
}

# ExchangeOrderNotification.NotifyType → NT OrderStatus
# Used for streaming execution events (template 352).
EXCHANGE_NOTIFY_TO_ORDER_STATUS: dict[int, OrderStatus] = {
    ExchangeOrderNotificationType.STATUS: OrderStatus.ACCEPTED,
    ExchangeOrderNotificationType.MODIFY: OrderStatus.ACCEPTED,
    ExchangeOrderNotificationType.CANCEL: OrderStatus.CANCELED,
    ExchangeOrderNotificationType.TRIGGER: OrderStatus.ACCEPTED,
    ExchangeOrderNotificationType.FILL: OrderStatus.FILLED,   # may be PARTIALLY_FILLED – checked at runtime
    ExchangeOrderNotificationType.REJECT: OrderStatus.REJECTED,
    ExchangeOrderNotificationType.NOT_MODIFIED: OrderStatus.ACCEPTED,
    ExchangeOrderNotificationType.NOT_CANCELLED: OrderStatus.ACCEPTED,
    ExchangeOrderNotificationType.GENERIC: OrderStatus.ACCEPTED,
}

# RithmicOrderNotification.NotifyType integer literals → NT OrderStatus
# These values come from rithmic_order_notification.proto and are NOT
# re-exported by the async_rithmic public API.
RITHMIC_NOTIFY_TO_ORDER_STATUS: dict[int, OrderStatus] = {
    1: OrderStatus.SUBMITTED,   # ORDER_RCVD_FROM_CLNT
    2: OrderStatus.SUBMITTED,   # MODIFY_RCVD_FROM_CLNT
    3: OrderStatus.SUBMITTED,   # CANCEL_RCVD_FROM_CLNT
    4: OrderStatus.SUBMITTED,   # OPEN_PENDING
    5: OrderStatus.ACCEPTED,    # MODIFY_PENDING
    6: OrderStatus.ACCEPTED,    # CANCEL_PENDING
    7: OrderStatus.SUBMITTED,   # ORDER_RCVD_BY_EXCH_GTWY
    8: OrderStatus.ACCEPTED,    # MODIFY_RCVD_BY_EXCH_GTWY
    9: OrderStatus.ACCEPTED,    # CANCEL_RCVD_BY_EXCH_GTWY
    10: OrderStatus.SUBMITTED,  # ORDER_SENT_TO_EXCH
    11: OrderStatus.ACCEPTED,   # MODIFY_SENT_TO_EXCH
    12: OrderStatus.ACCEPTED,   # CANCEL_SENT_TO_EXCH
    13: OrderStatus.ACCEPTED,   # OPEN
    14: OrderStatus.ACCEPTED,   # MODIFIED
    15: OrderStatus.FILLED,     # COMPLETE (check fill qty at runtime)
    16: OrderStatus.ACCEPTED,   # MODIFICATION_FAILED
    17: OrderStatus.ACCEPTED,   # CANCELLATION_FAILED
    18: OrderStatus.ACCEPTED,   # TRIGGER_PENDING
    19: OrderStatus.ACCEPTED,   # GENERIC
    20: OrderStatus.REJECTED,   # LINK_ORDERS_FAILED
}

# Liquidity side is not reported by Rithmic; use NO_LIQUIDITY_SIDE.
DEFAULT_LIQUIDITY_SIDE: LiquiditySide = LiquiditySide.NO_LIQUIDITY_SIDE


def ssboe_usecs_to_nanos(ssboe: int, usecs: int) -> int:
    """Convert Rithmic ``ssboe`` (seconds since UNIX epoch) + ``usecs`` to nanoseconds."""
    return ssboe * 1_000_000_000 + usecs * 1_000
