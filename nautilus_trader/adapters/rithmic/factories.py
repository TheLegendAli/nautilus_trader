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
Factory for the Rithmic live execution client.

Usage in a run script
---------------------
::

    from nautilus_trader.live.node import TradingNode
    from nautilus_trader.adapters.rithmic.factories import RithmicLiveExecClientFactory
    from nautilus_trader.adapters.rithmic.config import (
        RithmicExecClientConfig,
        RithmicInstrumentProviderConfig,
        RithmicInstrumentSpec,
    )

    config = TradingNodeConfig(
        ...
        exec_clients={
            "RITHMIC": RithmicExecClientConfig(
                user="my_user",
                password="my_pass",
                system_name="Rithmic Test",
                app_name="MyApp",
                app_version="1.0",
                url="wss://rituz00100.rithmic.com:443",
                account_id="F.000.000.0001",
                instrument_provider=RithmicInstrumentProviderConfig(
                    instruments=(
                        RithmicInstrumentSpec(
                            symbol="ESZ4",
                            exchange="CME",
                            currency="USD",
                            price_precision=2,
                            price_increment=0.25,
                            multiplier=50.0,
                            underlying="ES",
                            expiration_date="2024-12-20",
                        ),
                    ),
                ),
            ),
        },
    )

    node = TradingNode(config=config)
    node.add_exec_client_factory("RITHMIC", RithmicLiveExecClientFactory)
    node.build()
    node.run()
"""

from __future__ import annotations

import asyncio
import os

from nautilus_trader.adapters.rithmic.client import RithmicClientWrapper
from nautilus_trader.adapters.rithmic.config import RithmicExecClientConfig
from nautilus_trader.adapters.rithmic.execution import RithmicLiveExecutionClient
from nautilus_trader.adapters.rithmic.providers import RithmicInstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import Logger
from nautilus_trader.common.component import MessageBus
from nautilus_trader.live.factories import LiveExecClientFactory
from nautilus_trader.model.identifiers import AccountId


# Module-level cache so a single RithmicClientWrapper is reused if the same
# credentials are used more than once (e.g. data + exec clients sharing a connection).
_RITHMIC_CLIENTS: dict[str, RithmicClientWrapper] = {}


def get_cached_rithmic_client(
    config: RithmicExecClientConfig,
    logger: Logger,
) -> RithmicClientWrapper:
    """
    Return a cached :class:`RithmicClientWrapper`, creating one if it does not exist.

    The cache key is the ``account_id`` field, which is unique per trading account.

    Parameters
    ----------
    config : RithmicExecClientConfig
        Adapter configuration carrying connection credentials.
    logger : Logger
        NautilusTrader logger for the wrapper.

    Returns
    -------
    RithmicClientWrapper
    """
    cache_key = config.account_id
    if cache_key not in _RITHMIC_CLIENTS:
        user = config.user or os.environ.get("RITHMIC_USER", "")
        password = config.password or os.environ.get("RITHMIC_PASSWORD", "")
        _RITHMIC_CLIENTS[cache_key] = RithmicClientWrapper(
            user=user,
            password=password,
            system_name=config.system_name,
            app_name=config.app_name,
            app_version=config.app_version,
            url=config.url,
            logger=logger,
        )
    return _RITHMIC_CLIENTS[cache_key]


class RithmicLiveExecClientFactory(LiveExecClientFactory):
    """
    Provides a :class:`RithmicLiveExecutionClient` factory.

    Registered with the :class:`TradingNode` via::

        node.add_exec_client_factory("RITHMIC", RithmicLiveExecClientFactory)
    """

    @staticmethod
    def create(  # type: ignore
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: RithmicExecClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> RithmicLiveExecutionClient:
        """
        Create a new :class:`RithmicLiveExecutionClient`.

        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            The event loop for the client.
        name : str
            The client name / venue label (e.g. ``"RITHMIC"``).
        config : RithmicExecClientConfig
            Adapter configuration.
        msgbus : MessageBus
            The message bus.
        cache : Cache
            The cache.
        clock : LiveClock
            The clock.

        Returns
        -------
        RithmicLiveExecutionClient
        """
        logger = Logger(name=f"RithmicClient.{name}")

        client = get_cached_rithmic_client(config=config, logger=logger)

        provider = RithmicInstrumentProvider(config=config.instrument_provider)

        account_id_str = config.account_id or os.environ.get("RITHMIC_ACCOUNT_ID", "") or name
        account_id = AccountId(f"{name}-{account_id_str}")

        return RithmicLiveExecutionClient(
            loop=loop,
            client=client,
            account_id=account_id,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
        )
