import asyncio
import json
import re
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, patch

from aioresponses import aioresponses
from aioresponses.core import RequestCall

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.wazirx import wazirx_constants as CONSTANTS, wazirx_web_utils as web_utils
from hummingbot.connector.exchange.wazirx.wazirx_exchange import WazirxExchange
from hummingbot.connector.test_support.exchange_connector_test import AbstractExchangeConnectorTests
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import get_new_client_order_id
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase

# from hummingbot.core.event.events import OrderFilledEvent
from hummingbot.core.event.events import MarketOrderFailureEvent


class WazirxExchangeTests(AbstractExchangeConnectorTests.ExchangeConnectorTests):

    @property
    def all_symbols_url(self):
        return web_utils.public_rest_url(path_url=CONSTANTS.EXCHANGE_INFO_PATH_URL, domain=self.exchange._domain)

    @property
    def latest_prices_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.TICKER_PRICE_CHANGE_PATH_URL, domain=self.exchange._domain)
        url = f"{url}?symbol={self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)}"
        return url

    @property
    def network_status_url(self):
        url = web_utils.private_rest_url(CONSTANTS.PING_PATH_URL, domain=self.exchange._domain)
        return url

    @property
    def trading_rules_url(self):
        url = web_utils.private_rest_url(CONSTANTS.EXCHANGE_INFO_PATH_URL, domain=self.exchange._domain)
        return url

    @property
    def order_creation_url(self):
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL, domain=self.exchange._domain)
        return url

    @property
    def balance_url(self):
        url = web_utils.private_rest_url(CONSTANTS.ACCOUNTS_PATH_URL, domain=self.exchange._domain)
        return url

    @property
    def all_symbols_request_mock_response(self):
        return {
            "symbols": [
                {
                    "symbol": '{}{}'.format(self.base_asset.lower(), self.quote_asset.lower()),
                    "status": "trading",
                    "baseAsset": self.base_asset.lower(),
                    "baseAssetPrecision": 8,
                    "quoteAsset": self.quote_asset.lower(),
                    "quotePrecision": 8,
                    "quoteAssetPrecision": 8,
                    "baseCommissionPrecision": 8,
                    "quoteCommissionPrecision": 8,
                    "orderTypes": [
                        "limit",
                        "stop_limit",
                        "limit_maker"
                    ],
                    "isSpotTradingAllowed": True,
                    "filters": [],
                },
            ],
            "timezone": "UTC",
            "serverTime": 1639598493658,
        }

    @property
    def latest_prices_request_mock_response(self):
        return {
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "baseAsset": self.base_asset,
            "quoteAsset": self.quote_asset,
            "openPrice": "99.00000000",
            "lowPrice": "0.10000000",
            "highPrice": "100.00000000",
            "lastPrice": str(self.expected_latest_price),
            "volume": "8913.30000000",
            "bidPrice": "100.00000000",
            "askPrice": "4.00000200",
            "at": 1588829734
        }

    @property
    def all_symbols_including_invalid_pair_mock_response(self) -> Tuple[str, Any]:
        response = {
            "symbols": [
                {
                    "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                    "status": "trading",
                    "baseAsset": self.base_asset,
                    "baseAssetPrecision": 8,
                    "quoteAsset": self.quote_asset,
                    "quotePrecision": 8,
                    "quoteAssetPrecision": 8,
                    "baseCommissionPrecision": 8,
                    "quoteCommissionPrecision": 8,
                    "orderTypes": [
                        "limit",
                        "stop_limit",
                        "limit_maker"
                    ],
                    "isSpotTradingAllowed": True,
                    "filters": []
                },
                {
                    "symbol": self.exchange_symbol_for_tokens("invalid", "pair"),
                    "status": "trading",
                    "baseAsset": "invalid",
                    "baseAssetPrecision": 8,
                    "quoteAsset": "pair",
                    "quotePrecision": 8,
                    "quoteAssetPrecision": 8,
                    "baseCommissionPrecision": 8,
                    "quoteCommissionPrecision": 8,
                    "orderTypes": [
                        "limit",
                        "stop_limit",
                        "limit_maker"
                    ],
                    "isSpotTradingAllowed": True,
                    "filters": []
                },
            ],
            "timezone": "UTC",
            "serverTime": 1639598493658,
        }

        return "invalid-pair", response

    @property
    def network_status_request_successful_mock_response(self):
        return {}

    @property
    def trading_rules_request_mock_response(self):
        return {
            "symbols": [
                {
                    "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                    "status": "trading",
                    "baseAsset": self.base_asset,
                    "baseAssetPrecision": 8,
                    "quoteAsset": self.quote_asset,
                    "quotePrecision": 8,
                    "quoteAssetPrecision": 8,
                    "orderTypes": [
                        "limit",
                        "stop_limit",
                        "limit_maker"
                    ],
                    "isSpotTradingAllowed": True,
                    "filters": [
                        {
                            "filterType": "PRICE_FILTER",
                            "minPrice": "0.00000100",
                            "maxPrice": "100000.00000000",
                            "tickSize": "0.00000100"
                        }
                    ]
                }
            ],
            "timezone": "UTC",
            "serverTime": 1565246363776
        }

    @property
    def trading_rules_request_erroneous_mock_response(self):
        return {
            "symbols": [
                {
                    "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                    "status": "trading",
                    "baseAsset": self.base_asset,
                    "baseAssetPrecision": 8,
                    "quoteAsset": self.quote_asset,
                    "quotePrecision": 8,
                    "quoteAssetPrecision": 8,
                    "orderTypes": [
                        "limit",
                        "stop_limit",
                        "limit_maker"
                    ],
                    "isSpotTradingAllowed": True
                }
            ],
            "timezone": "UTC",
            "serverTime": 1565246363776,
        }

    @property
    def order_creation_request_successful_mock_response(self):
        return {
            "id": self.expected_exchange_order_id,
            "clientOrderId": "OID1",
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "price": "9293.0",
            "origQty": "99.0",
            "executedQty": "8.2",
            "status": "wait",
            "type": "limit",
            "side": "sell",
            "createdTime": 1499827319559,
            "updatedTime": 1499827319559
        }

    @property
    def balance_request_mock_response_for_base_and_quote(self):
        return [
            {
                "asset": self.base_asset,
                "free": "10.0",
                "locked": "5.0"
            },
            {
                "asset": self.quote_asset,
                "free": "2000",
                "locked": "0.00000000"
            }
        ]

    @property
    def balance_request_mock_response_only_base(self):
        return [
            {
                "asset": self.base_asset,
                "free": "10.0",
                "locked": "5.0"
            }
        ]

    @property
    def balance_event_websocket_update(self):
        return {
            "data":
                {
                    "B": [{"a": self.base_asset, "b": "10.0", "l": "5.0"}],
                    "E": 1631683058909
                },
                "stream": "outboundAccountPosition"
        }

    @property
    def expected_latest_price(self):
        return 9999.9

    @property
    def expected_supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    @property
    def expected_trading_rule(self):
        return TradingRule(
            trading_pair=self.trading_pair,
            min_order_size=Decimal(0.0001),
            min_price_increment=Decimal(
                self.trading_rules_request_mock_response["symbols"][0]["filters"][0]["minPrice"]),
            min_base_amount_increment=Decimal(
                self.trading_rules_request_mock_response["symbols"][0]["filters"][0]["tickSize"])
        )

    @property
    def expected_logged_error_for_erroneous_trading_rule(self):
        erroneous_rule = self.trading_rules_request_erroneous_mock_response["symbols"][0]
        return f"Error parsing the trading pair rule {erroneous_rule}. Skipping."

    @property
    def expected_exchange_order_id(self):
        return 28

    @property
    def is_order_fill_http_update_included_in_status_update(self) -> bool:
        return True

    @property
    def is_order_fill_http_update_executed_during_websocket_order_event_processing(self) -> bool:
        return False

    @property
    def expected_partial_fill_price(self) -> Decimal:
        return Decimal(10500)

    @property
    def expected_partial_fill_amount(self) -> Decimal:
        return Decimal("0.5")

    @property
    def expected_fill_fee(self) -> TradeFeeBase:
        return DeductedFromReturnsTradeFee(
            percent_token=self.quote_asset,
            flat_fees=[TokenAmount(token=self.quote_asset, amount=Decimal("30"))])

    @property
    def expected_fill_trade_id(self) -> str:
        return str(30000)

    def exchange_symbol_for_tokens(self, base_token: str, quote_token: str) -> str:
        return f"{base_token}{quote_token}"

    def create_exchange_instance(self):
        client_config_map = ClientConfigAdapter(ClientConfigMap())
        return WazirxExchange(
            client_config_map=client_config_map,
            wazirx_api_key="testAPIKey",
            wazirx_api_secret="testSecret",
            trading_pairs=[self.trading_pair],
        )

    def validate_auth_credentials_present(self, request_call: RequestCall):
        self._validate_auth_credentials_taking_parameters_from_argument(
            request_call_tuple=request_call,
            params=request_call.kwargs["params"] or request_call.kwargs["data"]
        )

    def validate_order_creation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = dict(request_call.kwargs["data"])
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), request_data["symbol"])
        self.assertEqual(order.trade_type.name.lower(), request_data["side"])
        self.assertEqual(WazirxExchange.wazirx_order_type(OrderType.LIMIT), request_data["type"])
        self.assertEqual(Decimal("100"), Decimal(request_data["quantity"]))
        self.assertEqual(Decimal("10000"), Decimal(request_data["price"]))
        self.assertEqual(order.client_order_id, request_data["clientOrderId"])

    def validate_order_cancelation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = dict(request_call.kwargs["params"])
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                         request_data["symbol"])
        self.assertEqual(order.client_order_id, request_data["clientOrderId"])

    def validate_order_status_request(self, order: InFlightOrder, request_call: RequestCall):
        request_params = request_call.kwargs["params"]
        self.assertEqual(order.client_order_id, request_params["clientOrderId"])

    def validate_trades_request(self, order: InFlightOrder, request_call: RequestCall):
        request_params = request_call.kwargs["params"]
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                         request_params["symbol"])
        self.assertEqual(order.exchange_order_id, str(request_params["orderId"]))

    def configure_successful_cancelation_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_cancelation_request_successful_mock_response(order=order)
        mock_api.delete(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_erroneous_cancelation_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.delete(regex_url, status=400, callback=callback)
        return url

    def configure_order_not_found_error_cancelation_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = {"code": -2011, "msg": "Unknown order sent."}
        mock_api.delete(regex_url, status=400, body=json.dumps(response), callback=callback)
        return url

    def configure_one_successful_one_erroneous_cancel_all_response(
            self,
            successful_order: InFlightOrder,
            erroneous_order: InFlightOrder,
            mock_api: aioresponses) -> List[str]:
        """
        :return: a list of all configured URLs for the cancelations
        """
        all_urls = []
        url = self.configure_successful_cancelation_response(order=successful_order, mock_api=mock_api)
        all_urls.append(url)
        url = self.configure_erroneous_cancelation_response(order=erroneous_order, mock_api=mock_api)
        all_urls.append(url)
        return all_urls

    def configure_completely_filled_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_status_request_completely_filled_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_canceled_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_status_request_canceled_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_erroneous_http_fill_trade_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.MY_TRADES_PATH_URL)
        regex_url = re.compile(url + r"\?.*")
        mock_api.get(regex_url, status=400, callback=callback)
        return url

    def configure_open_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        """
        :return: the URL configured
        """
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_status_request_open_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_http_error_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(regex_url, status=401, callback=callback)
        return url

    def configure_partially_filled_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_status_request_partially_filled_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_order_not_found_error_order_status_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> List[str]:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = {"code": -2013, "msg": "Order does not exist."}
        mock_api.get(regex_url, body=json.dumps(response), status=400, callback=callback)
        return [url]

    def configure_partial_fill_trade_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.MY_TRADES_PATH_URL)
        regex_url = re.compile(url + r"\?.*")
        response = self._order_fills_request_partial_fill_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_full_fill_trade_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.MY_TRADES_PATH_URL)
        regex_url = re.compile(url + r"\?.*")
        response = self._order_fills_request_full_fill_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def order_event_for_new_order_websocket_update(self, order: InFlightOrder):
        return {
            "data":
            {
                "E": 1499405658658,
                "O": 1499405658658,
                "S": order.order_type.name.lower(),
                "V": "0.00000000",
                "X": "wait",
                "i": order.exchange_order_id,
                "c": order.client_order_id,
                "m": True,
                "o": order.trade_type.name.lower(),
                "p": str(order.price),
                "q": str(order.amount),
                "s": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "v": "0.00000000",
                "z": "0.00000000"
            },
            "stream": "orderUpdate"
        }

    def order_event_for_canceled_order_websocket_update(self, order: InFlightOrder):
        return {
            "data":
            {
                "E": 1499405658658,
                "O": 1499405658658,
                "S": order.order_type.name.lower(),
                "V": "0.00000000",
                "X": "cancel",
                "i": order.exchange_order_id,
                "c": order.client_order_id,
                "m": True,
                "o": order.trade_type.name.lower(),
                "p": str(order.price),
                "q": str(order.amount),
                "s": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "v": "0.00000000",
                "z": "10000.00000000"
            },
            "stream": "orderUpdate"
        }

    def order_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {
            "data":
            {
                "E": 1499405658658,
                "O": 1499405658658,
                "S": order.order_type.name.lower(),
                "V": "10050.00000000",
                "X": "done",
                "i": order.exchange_order_id,
                "c": order.client_order_id,
                "m": True,
                "o": order.trade_type.name.lower(),
                "p": str(order.price),
                "q": str(order.amount),
                "s": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "v": "10000.00000000",
                "z": "10000.00000000"
            },
            "stream": "orderUpdate"
        }

    def trade_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {
            "data":
            {
                "E": 1499405658658,
                "S": order.order_type.name.lower(),
                "U": self.expected_fill_fee.flat_fees[0].token,
                "a": 114144050,
                "b": 114144121,
                "f": str(self.expected_fill_fee.flat_fees[0].amount),
                "m": True,
                "o": order.exchange_order_id,
                "c": order.client_order_id,
                "p": str(order.price),
                "q": str(order.amount),
                "s": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "t": 1,
                "w": "10000.0",
            },
            "stream": "ownTrade"
        }

    @aioresponses()
    @patch("hummingbot.connector.time_synchronizer.TimeSynchronizer._current_seconds_counter")
    def test_update_time_synchronizer_successfully(self, mock_api, seconds_counter_mock):
        request_sent_event = asyncio.Event()
        seconds_counter_mock.side_effect = [0, 0, 0]

        self.exchange._time_synchronizer.clear_time_offset_ms_samples()
        url = web_utils.private_rest_url(CONSTANTS.SERVER_TIME_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        response = {"serverTime": 1640000003000}

        mock_api.get(regex_url,
                     body=json.dumps(response),
                     callback=lambda *args, **kwargs: request_sent_event.set())

        self.async_run_with_timeout(self.exchange._update_time_synchronizer())

        self.assertEqual(response["serverTime"] * 1e-3, self.exchange._time_synchronizer.time())

    @aioresponses()
    def test_update_time_synchronizer_failure_is_logged(self, mock_api):
        request_sent_event = asyncio.Event()

        url = web_utils.private_rest_url(CONSTANTS.SERVER_TIME_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        response = {"code": -1121, "msg": "Dummy error"}

        mock_api.get(regex_url,
                     body=json.dumps(response),
                     callback=lambda *args, **kwargs: request_sent_event.set())

        self.async_run_with_timeout(self.exchange._update_time_synchronizer())

        self.assertTrue(self.is_logged("NETWORK", "Error getting server time."))

    @aioresponses()
    def test_update_time_synchronizer_raises_cancelled_error(self, mock_api):
        url = web_utils.private_rest_url(CONSTANTS.SERVER_TIME_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_api.get(regex_url,
                     exception=asyncio.CancelledError)

        self.assertRaises(
            asyncio.CancelledError,
            self.async_run_with_timeout, self.exchange._update_time_synchronizer())

    # @aioresponses()
    # def test_update_order_fills_from_trades_triggers_filled_event(self, mock_api):
    #     self.exchange._set_current_timestamp(1640780000)
    #     self.exchange._last_poll_timestamp = (self.exchange.current_timestamp -
    #                                           self.exchange.UPDATE_ORDER_STATUS_MIN_INTERVAL - 1)

    #     self.exchange.start_tracking_order(
    #         order_id="OID1",
    #         exchange_order_id="100234",
    #         trading_pair=self.trading_pair,
    #         order_type=OrderType.LIMIT,
    #         trade_type=TradeType.BUY,
    #         price=Decimal("10000"),
    #         amount=Decimal("1"),
    #     )
    #     order = self.exchange.in_flight_orders["OID1"]

    #     url = web_utils.private_rest_url(CONSTANTS.MY_TRADES_PATH_URL)
    #     regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

    #     trade_fill = {
    #         "id": 28457,
    #         "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
    #         "fee": "10.10000000",
    #         "feeCurrency": self.quote_asset,
    #         "quoteQty": "48.000012",
    #         "price": "9999",
    #         "qty": "1",
    #         "orderId": int(order.exchange_order_id),
    #         "side": "buy",
    #         "isBuyerMaker": True,
    #         "time": 1499865549590
    #     }

    #     trade_fill_non_tracked_order = {
    #         "id": 30000,
    #         "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
    #         "fee": "10.10000000",
    #         "feeCurrency": "inr",
    #         "quoteQty": "48.000012",
    #         "price": "4.00000100",
    #         "qty": "12.00000000",
    #         "orderId": 99999,
    #         "side": "buy",
    #         "isBuyerMaker": True,
    #         "time": 1499865549590
    #     }

    #     mock_response = [trade_fill, trade_fill_non_tracked_order]
    #     mock_api.get(regex_url, body=json.dumps(mock_response))

    #     self.exchange.add_exchange_order_ids_from_market_recorder(
    #         {str(trade_fill_non_tracked_order["orderId"]): "OID99"})

    #     self.async_run_with_timeout(self.exchange._update_order_fills_from_trades())

    #     request = self._all_executed_requests(mock_api, url)[0]
    #     self.validate_auth_credentials_present(request)
    #     request_params = request.kwargs["params"]
    #     self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), request_params["symbol"])

    #     fill_event: OrderFilledEvent = self.order_filled_logger.event_log[0]
    #     self.assertEqual(self.exchange.current_timestamp, fill_event.timestamp)
    #     self.assertEqual(order.client_order_id, fill_event.order_id)
    #     self.assertEqual(order.trading_pair, fill_event.trading_pair)
    #     self.assertEqual(order.trade_type, fill_event.trade_type)
    #     self.assertEqual(order.order_type, fill_event.order_type)
    #     self.assertEqual(Decimal(trade_fill["price"]), fill_event.price)
    #     self.assertEqual(Decimal(trade_fill["qty"]), fill_event.amount)
    #     self.assertEqual(0.0, fill_event.trade_fee.percent)
    #     self.assertEqual([TokenAmount(trade_fill["feeCurrency"], Decimal(trade_fill["fee"]))],
    #                      fill_event.trade_fee.flat_fees)

    #     fill_event: OrderFilledEvent = self.order_filled_logger.event_log[1]
    #     self.assertEqual(float(trade_fill_non_tracked_order["time"]) * 1e-3, fill_event.timestamp)
    #     self.assertEqual("OID99", fill_event.order_id)
    #     self.assertEqual(self.trading_pair, fill_event.trading_pair)
    #     self.assertEqual(TradeType.BUY, fill_event.trade_type)
    #     self.assertEqual(OrderType.LIMIT, fill_event.order_type)
    #     self.assertEqual(Decimal(trade_fill_non_tracked_order["price"]), fill_event.price)
    #     self.assertEqual(Decimal(trade_fill_non_tracked_order["qty"]), fill_event.amount)
    #     self.assertEqual(0.0, fill_event.trade_fee.percent)
    #     self.assertEqual([
    #         TokenAmount(
    #             trade_fill_non_tracked_order["feeCurrency"],
    #             Decimal(trade_fill_non_tracked_order["fee"]))],
    #         fill_event.trade_fee.flat_fees)
    #     self.assertTrue(self.is_logged(
    #         "INFO",
    #         f"Recreating missing trade in TradeFill: {trade_fill_non_tracked_order}"
    #     ))

    # @aioresponses()
    # def test_update_order_fills_request_parameters(self, mock_api):
    #     self.exchange._set_current_timestamp(0)
    #     self.exchange._last_poll_timestamp = -1

    #     url = web_utils.private_rest_url(CONSTANTS.MY_TRADES_PATH_URL)
    #     regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

    #     mock_response = []
    #     mock_api.get(regex_url, body=json.dumps(mock_response))

    #     self.async_run_with_timeout(self.exchange._update_order_fills_from_trades())

    #     request = self._all_executed_requests(mock_api, url)[0]
    #     self.validate_auth_credentials_present(request)
    #     request_params = request.kwargs["params"]
    #     self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), request_params["symbol"])
    #     self.assertNotIn("startTime", request_params)

    #     self.exchange._set_current_timestamp(1640780000)
    #     self.exchange._last_poll_timestamp = (self.exchange.current_timestamp -
    #                                           self.exchange.UPDATE_ORDER_STATUS_MIN_INTERVAL - 1)
    #     self.exchange._last_trades_poll_wazirx_timestamp = 10
    #     self.async_run_with_timeout(self.exchange._update_order_fills_from_trades())

    #     request = self._all_executed_requests(mock_api, url)[1]
    #     self.validate_auth_credentials_present(request)
    #     request_params = request.kwargs["params"]
    #     self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), request_params["symbol"])
    #     self.assertEqual(10 * 1e3, request_params["startTime"])

    # @aioresponses()
    # def test_update_order_fills_from_trades_with_repeated_fill_triggers_only_one_event(self, mock_api):
    #     self.exchange._set_current_timestamp(1640780000)
    #     self.exchange._last_poll_timestamp = (self.exchange.current_timestamp -
    #                                           self.exchange.UPDATE_ORDER_STATUS_MIN_INTERVAL - 1)

    #     url = web_utils.private_rest_url(CONSTANTS.MY_TRADES_PATH_URL)
    #     regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

    #     trade_fill_non_tracked_order = {
    #         "id": 30000,
    #         "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
    #         "fee": "10.10000000",
    #         "feeCurrency": "inr",
    #         "quoteQty": "48.000012",
    #         "price": "4.00000100",
    #         "qty": "12.00000000",
    #         "orderId": 99999,
    #         "side": "buy",
    #         "isBuyerMaker": True,
    #         "time": 1499865549590
    #     }

    #     mock_response = [trade_fill_non_tracked_order, trade_fill_non_tracked_order]
    #     mock_api.get(regex_url, body=json.dumps(mock_response))

    #     self.exchange.add_exchange_order_ids_from_market_recorder(
    #         {str(trade_fill_non_tracked_order["orderId"]): "OID99"})

    #     self.async_run_with_timeout(self.exchange._update_order_fills_from_trades())

    #     request = self._all_executed_requests(mock_api, url)[0]
    #     self.validate_auth_credentials_present(request)
    #     request_params = request.kwargs["params"]
    #     self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), request_params["symbol"])

    #     self.assertEqual(1, len(self.order_filled_logger.event_log))
    #     fill_event: OrderFilledEvent = self.order_filled_logger.event_log[0]
    #     self.assertEqual(float(trade_fill_non_tracked_order["time"]) * 1e-3, fill_event.timestamp)
    #     self.assertEqual("OID99", fill_event.order_id)
    #     self.assertEqual(self.trading_pair, fill_event.trading_pair)
    #     self.assertEqual(TradeType.BUY, fill_event.trade_type)
    #     self.assertEqual(OrderType.LIMIT, fill_event.order_type)
    #     self.assertEqual(Decimal(trade_fill_non_tracked_order["price"]), fill_event.price)
    #     self.assertEqual(Decimal(trade_fill_non_tracked_order["qty"]), fill_event.amount)
    #     self.assertEqual(0.0, fill_event.trade_fee.percent)
    #     self.assertEqual([
    #         TokenAmount(trade_fill_non_tracked_order["feeCurrency"],
    #                     Decimal(trade_fill_non_tracked_order["fee"]))],
    #         fill_event.trade_fee.flat_fees)
    #     self.assertTrue(self.is_logged(
    #         "INFO",
    #         f"Recreating missing trade in TradeFill: {trade_fill_non_tracked_order}"
    #     ))

    @aioresponses()
    def test_update_order_status_when_failed(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange._last_poll_timestamp = (self.exchange.current_timestamp -
                                              self.exchange.UPDATE_ORDER_STATUS_MIN_INTERVAL - 1)

        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="100234",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order = self.exchange.in_flight_orders["OID1"]

        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        order_status = {
            "id": int(order.exchange_order_id),
            "clientOrderId": order.client_order_id,
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "price": "10000.0",
            "stopPrice": "0.0",
            "origQty": "1.0",
            "executedQty": "0.0",
            "status": "failed",
            "type": "limit",
            "side": "buy",
            "createdTime": 1499827319559,
            "updatedTime": 1499827319559
        }

        mock_response = order_status
        mock_api.get(regex_url, body=json.dumps(mock_response))

        self.async_run_with_timeout(self.exchange._update_order_status())

        request = self._all_executed_requests(mock_api, url)[0]
        self.validate_auth_credentials_present(request)
        request_params = request.kwargs["params"]
        self.assertEqual(order.client_order_id, request_params["clientOrderId"])

        failure_event: MarketOrderFailureEvent = self.order_failure_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, failure_event.timestamp)
        self.assertEqual(order.client_order_id, failure_event.order_id)
        self.assertEqual(order.order_type, failure_event.order_type)
        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
        self.assertTrue(
            self.is_logged(
                "INFO",
                f"Order {order.client_order_id} has failed. Order Update: OrderUpdate(trading_pair='{self.trading_pair}',"
                f" update_timestamp={order_status['updatedTime'] * 1e-3}, new_state={repr(OrderState.FAILED)}, "
                f"client_order_id='{order.client_order_id}', exchange_order_id='{order.exchange_order_id}', "
                "misc_updates=None)")
        )

    def test_user_stream_update_for_order_failure(self):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="100234",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order = self.exchange.in_flight_orders["OID1"]

        event_message = {
            "data":
            {
                "E": 1499405658658,
                "O": 1499405658658,
                "S": "buy",
                "V": "0.00000000",
                "X": "failed",
                "i": int(order.exchange_order_id),
                "c": order.client_order_id,
                "m": True,
                "o": "limit",
                "p": "1000.00000000",
                "q": "1.00000000",
                "s": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "v": "0.0",
                "z": "0.0",
            },
            "stream": "orderUpdate"
        }

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [event_message, asyncio.CancelledError]
        self.exchange._user_stream_tracker._user_stream = mock_queue

        try:
            self.async_run_with_timeout(self.exchange._user_stream_event_listener())
        except asyncio.CancelledError:
            pass

        failure_event: MarketOrderFailureEvent = self.order_failure_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, failure_event.timestamp)
        self.assertEqual(order.client_order_id, failure_event.order_id)
        self.assertEqual(order.order_type, failure_event.order_type)
        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
        self.assertTrue(order.is_failure)
        self.assertTrue(order.is_done)

    @patch("hummingbot.connector.utils.get_tracking_nonce")
    def test_client_order_id_on_order(self, mocked_nonce):
        mocked_nonce.return_value = 7

        result = self.exchange.buy(
            trading_pair=self.trading_pair,
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
            price=Decimal("2"),
        )
        expected_client_order_id = get_new_client_order_id(
            is_buy=True,
            trading_pair=self.trading_pair,
            hbot_order_id_prefix=CONSTANTS.HBOT_ORDER_ID_PREFIX,
            max_id_len=CONSTANTS.MAX_ORDER_ID_LEN,
        )

        self.assertEqual(result, expected_client_order_id)

        result = self.exchange.sell(
            trading_pair=self.trading_pair,
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
            price=Decimal("2"),
        )
        expected_client_order_id = get_new_client_order_id(
            is_buy=False,
            trading_pair=self.trading_pair,
            hbot_order_id_prefix=CONSTANTS.HBOT_ORDER_ID_PREFIX,
            max_id_len=CONSTANTS.MAX_ORDER_ID_LEN,
        )

        self.assertEqual(result, expected_client_order_id)

    def test_time_synchronizer_related_request_error_detection(self):
        exception = IOError("Error executing request POST https://api.wazirx.com/api/v3/order. HTTP status is 400. "
                            "Error: {'code':-1021,'msg':'Timestamp for this request is outside of the recvWindow.'}")
        self.assertTrue(self.exchange._is_request_exception_related_to_time_synchronizer(exception))

        exception = IOError("Error executing request POST https://api.wazirx.com/api/v3/order. HTTP status is 400. "
                            "Error: {'code':-1021,'msg':'Timestamp for this request was 1000ms ahead of the server's "
                            "time.'}")
        self.assertTrue(self.exchange._is_request_exception_related_to_time_synchronizer(exception))

        exception = IOError("Error executing request POST https://api.wazirx.com/api/v3/order. HTTP status is 400. "
                            "Error: {'code':-1022,'msg':'Timestamp for this request was 1000ms ahead of the server's "
                            "time.'}")
        self.assertFalse(self.exchange._is_request_exception_related_to_time_synchronizer(exception))

        exception = IOError("Error executing request POST https://api.wazirx.com/api/v3/order. HTTP status is 400. "
                            "Error: {'code':-1021,'msg':'Other error.'}")
        self.assertFalse(self.exchange._is_request_exception_related_to_time_synchronizer(exception))

    @aioresponses()
    def test_place_order_manage_server_overloaded_error_unkown_order(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange._last_poll_timestamp = (self.exchange.current_timestamp -
                                              self.exchange.UPDATE_ORDER_STATUS_MIN_INTERVAL - 1)
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_response = {"code": -1003, "msg": "Unknown error, please check your request or try again later."}
        mock_api.post(regex_url, body=json.dumps(mock_response), status=503)

        o_id, transact_time = self.async_run_with_timeout(self.exchange._place_order(
            order_id="test_order_id",
            trading_pair=self.trading_pair,
            amount=Decimal("1"),
            trade_type=TradeType.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("2"),
        ))
        self.assertEqual(o_id, "UNKNOWN")

    @aioresponses()
    def test_place_order_manage_server_overloaded_error_failure(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange._last_poll_timestamp = (self.exchange.current_timestamp -
                                              self.exchange.UPDATE_ORDER_STATUS_MIN_INTERVAL - 1)

        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_response = {"code": -1003, "msg": "Service Unavailable."}
        mock_api.post(regex_url, body=json.dumps(mock_response), status=503)

        self.assertRaises(
            IOError,
            self.async_run_with_timeout,
            self.exchange._place_order(
                order_id="test_order_id",
                trading_pair=self.trading_pair,
                amount=Decimal("1"),
                trade_type=TradeType.BUY,
                order_type=OrderType.LIMIT,
                price=Decimal("2"),
            ))

        mock_response = {"code": -1003, "msg": "Internal error; unable to process your request. Please try again."}
        mock_api.post(regex_url, body=json.dumps(mock_response), status=503)

        self.assertRaises(
            IOError,
            self.async_run_with_timeout,
            self.exchange._place_order(
                order_id="test_order_id",
                trading_pair=self.trading_pair,
                amount=Decimal("1"),
                trade_type=TradeType.BUY,
                order_type=OrderType.LIMIT,
                price=Decimal("2"),
            ))

    def _validate_auth_credentials_taking_parameters_from_argument(self,
                                                                   request_call_tuple: RequestCall,
                                                                   params: Dict[str, Any]):
        self.assertIn("timestamp", params)
        self.assertIn("signature", params)
        request_headers = request_call_tuple.kwargs["headers"]
        self.assertIn("X-API-KEY", request_headers)
        self.assertEqual("testAPIKey", request_headers["X-API-KEY"])

    def _order_cancelation_request_successful_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "id": 4,
            "clientOrderId": order.client_order_id,
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "price": str(order.price),
            "origQty": str(order.amount),
            "executedQty": str(Decimal("0")),
            "status": "wait",
            "type": "limit",
            "side": "buy",
            "createdTime": 1499827319559,
            "updatedTime": 1499827319559
        }

    def _order_status_request_completely_filled_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "id": order.exchange_order_id,
            "clientOrderId": order.client_order_id,
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "price": str(order.price),
            "stopPrice": "0.0",
            "origQty": str(order.amount),
            "executedQty": str(order.amount),
            "status": "done",
            "type": "limit",
            "side": "buy",
            "createdTime": 1499827319559,
            "updatedTime": 1499827319559
        }

    def _order_status_request_canceled_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "id": order.exchange_order_id,
            "clientOrderId": order.client_order_id,
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "price": str(order.price),
            "stopPrice": "0.0",
            "origQty": str(order.amount),
            "executedQty": str(order.amount),
            "status": "cancel",
            "type": order.trade_type.name.lower(),
            "side": order.order_type.name.lower(),
            "createdTime": 1499827319559,
            "updatedTime": 1499827319559
        }

    def _order_status_request_open_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "id": order.exchange_order_id,
            "clientOrderId": order.client_order_id,
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "price": str(order.price),
            "stopPrice": "0.0",
            "origQty": str(order.amount),
            "executedQty": str(order.amount),
            "status": "idle",
            "type": order.trade_type.name.lower(),
            "side": order.order_type.name.lower(),
            "createdTime": 1499827319559,
            "updatedTime": 1499827319559
        }

    def _order_status_request_partially_filled_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "id": order.exchange_order_id,
            "clientOrderId": order.client_order_id,
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "price": str(order.price),
            "stopPrice": "0.0",
            "origQty": str(order.amount),
            "executedQty": str(order.amount),
            "status": "wait",
            "type": order.trade_type.name.lower(),
            "side": order.order_type.name.lower(),
            "createdTime": 1499827319559,
            "updatedTime": 1499827319559
        }

    def _order_fills_request_partial_fill_mock_response(self, order: InFlightOrder):
        return [
            {
                "id": self.expected_fill_trade_id,
                "symbol": self.exchange_symbol_for_tokens(order.base_asset, order.quote_asset),
                "fee": str(self.expected_fill_fee.flat_fees[0].amount),
                "feeCurrency": self.expected_fill_fee.flat_fees[0].token,
                "quoteQty": str(self.expected_partial_fill_amount * self.expected_partial_fill_price),
                "price": str(self.expected_partial_fill_price),
                "qty": str(self.expected_partial_fill_amount),
                "orderId": int(order.exchange_order_id),
                "side": order.order_type.name.lower(),
                "isBuyerMaker": True,
                "time": 1499865549590
            }

        ]

    def _order_fills_request_full_fill_mock_response(self, order: InFlightOrder):
        return [
            {
                "id": self.expected_fill_trade_id,
                "symbol": self.exchange_symbol_for_tokens(order.base_asset, order.quote_asset),
                "fee": str(self.expected_fill_fee.flat_fees[0].amount),
                "feeCurrency": self.expected_fill_fee.flat_fees[0].token,
                "quoteQty": str(order.amount * order.price),
                "price": str(order.price),
                "qty": str(order.amount),
                "orderId": int(order.exchange_order_id),
                "side": order.order_type.name.lower(),
                "isBuyerMaker": True,
                "time": 1499865549590
            }
        ]
