import asyncio
from typing import TYPE_CHECKING, Any, Dict, Optional

from hummingbot.connector.exchange.wazirx import wazirx_constants as CONSTANTS, wazirx_web_utils as web_utils
from hummingbot.connector.exchange.wazirx.wazirx_auth import WazirxAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.wazirx.wazirx_exchange import WazirxExchange


class WazirxAPIUserStreamDataSource(UserStreamTrackerDataSource):
    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: WazirxAuth,
                 connector: 'WazirxExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):

        super().__init__()
        self._auth: WazirxAuth = auth
        self._api_factory = api_factory
        self._connector = connector
        self._domain = domain
        self._current_auth_token: Optional[str] = None
        self._api_factory = api_factory

        self._listen_key_initialized_event: asyncio.Event = asyncio.Event()
        self._last_listen_key_ping_ts = 0

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WSS_URL, ping_timeout=CONSTANTS.PING_TIMEOUT)
        return ws

    @property
    def last_recv_time(self):
        if self._ws_assistant is None:
            return 0
        else:
            return self._ws_assistant.last_recv_time

    async def get_auth_token(self) -> str:
        rest_assistant = await self._api_factory.get_rest_assistant()
        try:
            response_json = await rest_assistant.execute_request(
                url=web_utils.public_rest_url(path_url=CONSTANTS.WAZIRX_USER_STREAM_PATH_URL, domain=self._domain),
                method=RESTMethod.POST,
                params={"recvWindow": 10000},
                is_auth_required=True,
                throttler_limit_id=CONSTANTS.WAZIRX_USER_STREAM_PATH_URL,
                headers=self._auth.header_for_authentication()
            )
        except Exception:
            raise
        return response_json["auth_key"]

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        try:
            self._current_listen_key = await self.get_auth_token()
            payload = {"event": "subscribe", "streams": ["orderUpdate", "ownTrade", "outboundAccountPosition"], "auth_key": self._current_listen_key}
            subscribe_request = WSJSONRequest(payload)
            await websocket_assistant.send(subscribe_request)

            self.logger().info("Subscribed to private order changes and trades updates channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error occurred subscribing to user streams...")
            raise

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        if "streams" or "data" in event_message or event_message["stream"] in [
            CONSTANTS.USER_TRADES_ENDPOINT_NAME,
            CONSTANTS.USER_ORDERS_ENDPOINT_NAME,
            CONSTANTS.USER_BALANCE_ENDPOINT_NAME
        ]:
            queue.put_nowait(event_message)
        else:
            if event_message.get("errorMessage") is not None:
                err_msg = event_message.get("errorMessage")
                raise IOError({
                    "label": "WSS_ERROR",
                    "message": f"Error received via websocket - {err_msg}."
                })

    async def _send_ping(self, websocket_assistant: WSAssistant):
        payload = {
            "event": "ping",
        }
        ping_request: WSJSONRequest = WSJSONRequest(payload=payload)
        await websocket_assistant.send(ping_request)
