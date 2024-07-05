from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = "wazirx"

HBOT_ORDER_ID_PREFIX = "x-XEKWYICX"
MAX_ORDER_ID_LEN = 32

# Base URL
REST_URL = "https://api.wazirx.com/sapi/"
WSS_URL = "wss://stream.wazirx.com/stream"

PUBLIC_API_VERSION = "v1"
PRIVATE_API_VERSION = "v1"

# Public API endpoints or WazirxClient function
TICKER_PRICE_CHANGE_PATH_URL = "/ticker/24hr"
TICKER_BOOK_PATH_URL = "/tickers/24hr"
EXCHANGE_INFO_PATH_URL = "/exchangeInfo"
PING_PATH_URL = "/ping"
SNAPSHOT_PATH_URL = "/depth"
SERVER_TIME_PATH_URL = "/time"

# Private API endpoints or WazirxClient function
ACCOUNTS_PATH_URL = "/funds"
MY_TRADES_PATH_URL = "/myTrades"
ORDER_PATH_URL = "/order"
WAZIRX_USER_STREAM_PATH_URL = "/create_auth_token"

WS_HEARTBEAT_TIME_INTERVAL = 30
PING_TIMEOUT = 10

ORDER_TYPE_MAP = {
    OrderType.LIMIT: "limit",
    OrderType.LIMIT_MAKER: "limit_maker"
}
# Wazirx params

SIDE_BUY = "buy"
SIDE_SELL = "sell"

# Rate Limit time intervals
ONE_MINUTE = 60
ONE_SECOND = 1
ONE_DAY = 86400

MAX_REQUEST = 5000

# Order States
ORDER_STATE = {
    "idle": OrderState.PENDING_CREATE,
    "wait": OrderState.PARTIALLY_FILLED,
    "done": OrderState.FILLED,
    "cancel": OrderState.CANCELED,
    "failed": OrderState.FAILED
}

# Websocket event types
DIFF_EVENT_TYPE = "{}@depth10@100ms"
TRADE_EVENT_TYPE = "{}@trades"

USER_TRADES_ENDPOINT_NAME = "ownTrade"
USER_ORDERS_ENDPOINT_NAME = "orderUpdate"
USER_BALANCE_ENDPOINT_NAME = "outboundAccountPosition"

RATE_LIMITS = [
    RateLimit(limit_id=PING_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=EXCHANGE_INFO_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=TICKER_PRICE_CHANGE_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=TICKER_BOOK_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=SNAPSHOT_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=WAZIRX_USER_STREAM_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=ACCOUNTS_PATH_URL, limit=10, time_interval=ONE_SECOND),
    RateLimit(limit_id=MY_TRADES_PATH_URL, limit=2, time_interval=ONE_SECOND),
    RateLimit(limit_id=ORDER_PATH_URL, limit=10, time_interval=ONE_SECOND),
]

ORDER_NOT_EXIST_ERROR_CODE = -2013
ORDER_NOT_EXIST_MESSAGE = "Order does not exist"
UNKNOWN_ORDER_ERROR_CODE = -2011
UNKNOWN_ORDER_MESSAGE = "Unknown order sent"
