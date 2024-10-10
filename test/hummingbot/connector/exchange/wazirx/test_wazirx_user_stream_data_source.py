
import asyncio
import json
import re
import unittest
from typing import Awaitable, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses import aioresponses
from bidict import bidict

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.wazirx import wazirx_constants as CONSTANTS, wazirx_web_utils as web_utils
from hummingbot.connector.exchange.wazirx.wazirx_api_user_stream_data_source import WazirxAPIUserStreamDataSource
from hummingbot.connector.exchange.wazirx.wazirx_auth import WazirxAuth
from hummingbot.connector.exchange.wazirx.wazirx_exchange import WazirxExchange
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler


class WazirxAPIUserStreamDataSourceTest(unittest.TestCase):
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = f"{cls.base_asset}{cls.quote_asset}"
        cls.listen_key = "TEST_LISTEN_KEY"
        cls.domain = "wazirx"

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []
        self.listening_task: Optional[asyncio.Task] = None
        self.mocking_assistant = NetworkMockingAssistant()

        self.throttler = AsyncThrottler(rate_limits=CONSTANTS.RATE_LIMITS)
        self.mock_time_provider = MagicMock()

        client_config_map = ClientConfigAdapter(ClientConfigMap())
        self.connector = WazirxExchange(
            client_config_map=client_config_map,
            wazirx_api_key="",
            wazirx_api_secret="",
            trading_pairs=[self.trading_pair],
            trading_required=False)

        not_a_real_secret = "kQH5HW/8p1uGOVjbgWA7FunAmGO8lsSUXNsu3eow76sz84Q18fWxnyRzBHCd3pd5nE9qa99HAZtuZuj6F1huXg=="
        self.auth = WazirxAuth(api_key="someKey", secret_key=not_a_real_secret, time_provider=self.mock_time_provider)

        self.connector._web_assistants_factory._auth = self.auth
        self.data_source = WazirxAPIUserStreamDataSource(self.auth,
                                                         self.connector,
                                                         api_factory=self.connector._web_assistants_factory,
                                                         )

        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        self.resume_test_event = asyncio.Event()

        self.connector._set_trading_pair_symbol_map(bidict({self.ex_trading_pair: self.trading_pair}))

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message
                   for record in self.log_records)

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    @staticmethod
    def get_auth_response_mock() -> Dict:
        auth_resp = {
            "auth_key": "1Dwc4lzSwNWOAwkMdqhssNNFhs1ed606d1WcF3XfEMw",
            "timeout_duration": 900
        }
        return auth_resp

    @staticmethod
    def get_open_orders_mock() -> List:
        open_orders = {
            "data":
            {
                "E": 1631683058904,
                "O": 1631683058000,
                "S": "sell",
                "V": "70.0",
                "X": "wait",
                "i": 26946170,
                "c": "my_clientorder_1",
                "m": True,
                "o": "limit",
                "p": "5.0",
                "q": "70.0",
                "s": "wrxinr",
                "v": "0.0",
                "z": "0.0"
            },
            "stream": "orderUpdate"
        }
        return open_orders

    @staticmethod
    def get_own_trades_mock() -> List:
        own_trades = {
            "data":
            {
                "E": 1631683058000,
                "S": "ask",
                "U": "inr",
                "a": 114144050,
                "b": 114144121,
                "f": "0.2",
                "m": True,
                "o": 26946170,
                "c": "my_clientorder_1",
                "p": "5.0",
                "q": "20.0",
                "s": "btcinr",
                "t": 17376032,
                "w": "100.0"
            },
            "stream": "ownTrade"
        }
        return own_trades

    @aioresponses()
    def test_get_auth_token(self, mocked_api):
        url = web_utils.private_rest_url(path_url=CONSTANTS.WAZIRX_USER_STREAM_PATH_URL, domain=self.domain)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        resp = self.get_auth_response_mock()
        mocked_api.post(regex_url, body=json.dumps(resp))

        ret = self.async_run_with_timeout(self.data_source.get_auth_token())

        self.assertEqual(ret, resp["auth_key"])

    @aioresponses()
    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream(self, mocked_api, ws_connect_mock):
        url = web_utils.private_rest_url(path_url=CONSTANTS.WAZIRX_USER_STREAM_PATH_URL, domain=self.domain)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        resp = self.get_auth_response_mock()
        mocked_api.post(regex_url, body=json.dumps(resp))
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()
        output_queue = asyncio.Queue()
        self.ev_loop.create_task(self.data_source.listen_for_user_stream(output_queue))

        resp = self.get_open_orders_mock()
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value, message=json.dumps(resp)
        )
        ret = self.async_run_with_timeout(coroutine=output_queue.get())

        self.assertEqual(ret, resp)

        resp = self.get_own_trades_mock()
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value, message=json.dumps(resp)
        )
        ret = self.async_run_with_timeout(coroutine=output_queue.get())

        self.assertEqual(ret, resp)
