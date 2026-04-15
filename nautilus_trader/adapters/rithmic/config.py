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

from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.config import LiveExecClientConfig
from nautilus_trader.config import NautilusConfig


class RithmicInstrumentSpec(NautilusConfig, frozen=True):
    """
    Specification for a single Rithmic-traded instrument used to pre-configure
    the instrument provider without requiring a live market-data connection.

    Parameters
    ----------
    symbol : str
        The Rithmic trading symbol (e.g. ``"ESZ4"``).
    exchange : str
        The Rithmic exchange code (e.g. ``"CME"``).
    currency : str
        The settlement currency ISO code (e.g. ``"USD"``).
    price_precision : int
        The number of decimal places for prices.
    price_increment : float
        The minimum tick size (e.g. ``0.25`` for ES).
    multiplier : float
        The contract multiplier (e.g. ``50.0`` for ES).
    underlying : str
        The underlying root symbol (e.g. ``"ES"``).
    expiration_date : str
        ISO-8601 expiry date string ``"YYYY-MM-DD"``.
    activation_date : str, optional
        ISO-8601 activation date string. Defaults to 90 days before expiry.
    size_precision : int, default 0
        The number of decimal places for quantities (typically 0 for futures).
    lot_size : float, default 1.0
        The minimum order lot size.
    asset_class : str, default ``"INDEX"``
        Nautilus asset class label (``"INDEX"``, ``"EQUITY"``, ``"FX"``,
        ``"COMMODITY"``, ``"METAL"``, ``"ENERGY"``, ``"BOND"``, ``"RATE"``).
    """

    symbol: str
    exchange: str
    currency: str
    price_precision: int
    price_increment: float
    multiplier: float
    underlying: str
    expiration_date: str
    activation_date: str | None = None
    size_precision: int = 0
    lot_size: float = 1.0
    asset_class: str = "INDEX"


class RithmicInstrumentProviderConfig(InstrumentProviderConfig, frozen=True):
    """
    Configuration for :class:`RithmicInstrumentProvider`.

    Parameters
    ----------
    instruments : tuple[RithmicInstrumentSpec, ...]
        Pre-configured instrument specs loaded at startup.
    """

    instruments: tuple[RithmicInstrumentSpec, ...] = ()


class RithmicExecClientConfig(LiveExecClientConfig, frozen=True):
    """
    Configuration for :class:`RithmicLiveExecutionClient`.

    Parameters
    ----------
    user : str
        Rithmic login username.
    password : str
        Rithmic login password.
    system_name : str
        Rithmic system name (e.g. ``"Rithmic Test"`` or ``"Rithmic 01"``).
    app_name : str
        Application name registered with Rithmic.
    app_version : str
        Application version string.
    url : str
        Rithmic gateway WebSocket URL.
    account_id : str
        The trading account ID.
    fcm_id : str, optional
        FCM identifier override. Derived from the connected session when ``None``.
    ib_id : str, optional
        IB identifier override. Derived from the connected session when ``None``.
    connection_timeout : int, default 30
        Seconds to wait for the ORDER and PNL plants to finish login.
    fill_history_days : int, default 0
        Number of past calendar days to back-fill fills from Rithmic order history
        on connect. ``0`` disables back-fill.
    instrument_provider : RithmicInstrumentProviderConfig, optional
        Instrument provider configuration.
    """

    user: str = ""
    password: str = ""
    system_name: str = ""
    app_name: str = ""
    app_version: str = ""
    url: str = ""
    account_id: str = ""
    fcm_id: str | None = None
    ib_id: str | None = None
    connection_timeout: int = 30
    fill_history_days: int = 0
    instrument_provider: RithmicInstrumentProviderConfig = RithmicInstrumentProviderConfig()
