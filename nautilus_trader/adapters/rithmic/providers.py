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
Instrument provider for the Rithmic adapter.

Because the user does not require a market-data connection, all instruments are
loaded from pre-configured :class:`RithmicInstrumentSpec` objects supplied in
:class:`RithmicInstrumentProviderConfig`.  No live Rithmic plant connection is
needed for instrument loading.
"""

from __future__ import annotations

from nautilus_trader.adapters.rithmic.config import RithmicInstrumentProviderConfig
from nautilus_trader.adapters.rithmic.parsing.instruments import make_instrument_id
from nautilus_trader.adapters.rithmic.parsing.instruments import spec_to_futures_contract
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.model.identifiers import InstrumentId


class RithmicInstrumentProvider(InstrumentProvider):
    """
    Provides :class:`FuturesContract` objects from pre-configured
    :class:`RithmicInstrumentSpec` entries.

    Parameters
    ----------
    config : RithmicInstrumentProviderConfig
        Configuration containing instrument specifications.
    """

    def __init__(self, config: RithmicInstrumentProviderConfig | None = None) -> None:
        super().__init__(config=config or RithmicInstrumentProviderConfig())
        self._config: RithmicInstrumentProviderConfig = config or RithmicInstrumentProviderConfig()

    async def load_all_async(self, filters: dict | None = None) -> None:
        """Load all instruments from the configuration specs."""
        for spec in self._config.instruments:
            instrument = spec_to_futures_contract(spec)
            self.add(instrument)
            self._log.info(f"Loaded instrument: {instrument.id}")

    async def load_async(self, instrument_id: InstrumentId, filters: dict | None = None) -> None:
        """Load a single instrument by ID if it matches a configured spec."""
        for spec in self._config.instruments:
            if make_instrument_id(spec.symbol, spec.exchange) == instrument_id:
                instrument = spec_to_futures_contract(spec)
                self.add(instrument)
                self._log.info(f"Loaded instrument: {instrument.id}")
                return
        self._log.warning(f"No spec found for instrument: {instrument_id}")
