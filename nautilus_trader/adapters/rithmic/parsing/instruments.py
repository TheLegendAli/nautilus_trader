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
Instrument parsing helpers: Rithmic exchange codes → MIC venue strings,
and :class:`RithmicInstrumentSpec` → :class:`FuturesContract`.
"""

import time

import pandas as pd

from nautilus_trader.adapters.rithmic.config import RithmicInstrumentSpec
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.enums import asset_class_from_str
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments import FuturesContract
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


# ---------------------------------------------------------------------------
# Rithmic exchange code → ISO 10383 MIC venue string
# ---------------------------------------------------------------------------

RITHMIC_EXCHANGE_TO_MIC: dict[str, str] = {
    # CME Group — use Databento venue codes so instrument IDs match live feed
    "CME": "GLBX",
    "CBOT": "GLBX",
    "NYMEX": "GLBX",
    "COMEX": "GLBX",
    # ICE
    "ICE": "IFUS",
    "ICEUS": "IFUS",
    "ICEEU": "IFEU",
    # Eurex
    "EUREX": "XEUR",
    # Euronext
    "EURONEXT": "XPAR",
    # CBOE
    "CFE": "XCBF",
    # SGX
    "SGX": "XSES",
    # HKEX
    "HKFE": "XHKF",
    # Osaka/JPX
    "OSE": "XOSE",
    # Korean Exchange
    "KRX": "XKRX",
    # ASX
    "SFE": "XSFE",
}


def exchange_to_mic_venue(exchange: str) -> str:
    """Return the MIC venue string for a Rithmic exchange code.

    Falls back to the exchange code itself when no mapping exists.
    """
    return RITHMIC_EXCHANGE_TO_MIC.get(exchange.upper(), exchange.upper())


def make_instrument_id(symbol: str, exchange: str) -> InstrumentId:
    """Build a NautilusTrader :class:`InstrumentId` from a Rithmic symbol/exchange pair."""
    venue_str = exchange_to_mic_venue(exchange)
    return InstrumentId(Symbol(symbol), Venue(venue_str))


def _asset_class(spec: RithmicInstrumentSpec) -> AssetClass:
    try:
        return asset_class_from_str(spec.asset_class.upper())
    except ValueError:
        return AssetClass.INDEX


def spec_to_futures_contract(spec: RithmicInstrumentSpec) -> FuturesContract:
    """Convert a :class:`RithmicInstrumentSpec` into a :class:`FuturesContract`."""
    instrument_id = make_instrument_id(spec.symbol, spec.exchange)
    currency = Currency.from_str(spec.currency)

    expiration_dt = pd.Timestamp(spec.expiration_date, tz="UTC")
    expiration_ns = expiration_dt.value

    if spec.activation_date is not None:
        activation_ns = pd.Timestamp(spec.activation_date, tz="UTC").value
    else:
        activation_ns = (expiration_dt - pd.Timedelta(days=90)).value

    price_precision = spec.price_precision
    ts_now = time.time_ns()

    return FuturesContract(
        instrument_id=instrument_id,
        raw_symbol=Symbol(spec.symbol),
        asset_class=_asset_class(spec),
        currency=currency,
        price_precision=price_precision,
        price_increment=Price(spec.price_increment, price_precision),
        multiplier=Quantity(spec.multiplier, precision=0),
        lot_size=Quantity(spec.lot_size, precision=spec.size_precision),
        underlying=spec.underlying,
        activation_ns=activation_ns,
        expiration_ns=expiration_ns,
        ts_event=ts_now,
        ts_init=ts_now,
    )
