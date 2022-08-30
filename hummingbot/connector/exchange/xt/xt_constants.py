# A single source of truth for constant variables related to the exchange

from hummingbot.core.api_throttler.data_types import RateLimit

EXCHANGE_NAME = "xt"
REST_URL = "https://api.xt.com"
WSS_URL = "wss://xtsocket.xt.com/websocket"

# REST API ENDPOINTS
CHECK_NETWORK_PATH_URL = "trade/api/v1/getServerTime"
GET_TRADING_PAIRS_PATH_URL = "data/api/v1/getTickers"
GET_TRADING_RULES_PATH_URL = "data/api/v1/getMarketConfig"
GET_LAST_TRADING_PRICES_PATH_URL = "data/api/v1/getTickers"
GET_ORDER_BOOK_PATH_URL = "data/api/v1/getDepth"
CREATE_ORDER_PATH_URL = "trade/api/v1/order"
CANCEL_ORDER_PATH_URL = "trade/api/v1/cancel"
BATCH_CANCEL_ORDER_PATH_URL = "trade/api/v1/batchCancel"
GET_ACCOUNT_SUMMARY_PATH_URL = "trade/api/v1/getBalance"
GET_ORDER_DETAIL_PATH_URL = "trade/api/v1/getBatchOrders"
GET_TRADE_DETAIL_PATH_URL = "trade/api/v1/myTrades"
GET_OPEN_ORDERS_PATH_URL = "trade/api/v1/getOpenOrders"

# WS API ENDPOINTS
WS_CONNECT = "WSConnect"
WS_SUBSCRIBE = "WSSubscribe"

# XT has a per method API limit
RATE_LIMITS = [
    RateLimit(limit_id=CHECK_NETWORK_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=GET_TRADING_PAIRS_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=GET_TRADING_RULES_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=GET_LAST_TRADING_PRICES_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=GET_ORDER_BOOK_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=CREATE_ORDER_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=CANCEL_ORDER_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=BATCH_CANCEL_ORDER_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=GET_ACCOUNT_SUMMARY_PATH_URL, limit=3, time_interval=1),
    RateLimit(limit_id=GET_ORDER_DETAIL_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=GET_TRADE_DETAIL_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=GET_OPEN_ORDERS_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=WS_CONNECT, limit=1, time_interval=1),
    RateLimit(limit_id=WS_SUBSCRIBE, limit=60, time_interval=600),
]

ORDER_STATUS = {
    0: "OPEN",          # submission not matched
    1: "ACTIVE",        # unsettled or partially completed
    2: "COMPLETED",     # completed
    3: "CANCELED",      # cancelled
    4: "ACTIVE",        # matched but in the settlement
}
