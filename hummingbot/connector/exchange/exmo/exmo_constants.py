# A single source of truth for constant variables related to the exchange

from hummingbot.core.api_throttler.data_types import RateLimit

EXCHANGE_NAME = "exmo"
REST_URL = "https://api.exmo.com/"
WSS_PUBLIC_URL = "wss://ws-api.exmo.com:443/v1/public"
WSS_PRIVATE_URL = "wss://ws-api.exmo.com:443/v1/private"

# REST API ENDPOINTS
CHECK_NETWORK_PATH_URL = "v1.1/currency"
GET_TRADING_PAIRS_PATH_URL = "v1.1/ticker"
GET_TRADING_RULES_PATH_URL = "v1.1/pair_settings"
GET_LAST_TRADING_PRICES_PATH_URL = "v1.1/ticker"
GET_ORDER_BOOK_PATH_URL = "v1.1/order_book"
CREATE_ORDER_PATH_URL = "v1.1/order_create"
CANCEL_ORDER_PATH_URL = "v1.1/order_cancel"
GET_ACCOUNT_SUMMARY_PATH_URL = "v1.1/user_info"
GET_ORDER_DETAIL_PATH_URL = "v1.1/order_trades"
GET_TRADE_DETAIL_PATH_URL = "v1.1/user_trades"
GET_OPEN_ORDERS_PATH_URL = "v1.1/user_open_orders"
GET_CANCELLED_ORDERS_PATH_URL = "v1.1/user_cancelled_orders"

# WS API ENDPOINTS
WS_CONNECT = "WSConnect"
WS_SUBSCRIBE = "WSSubscribe"

RATE_LIMITS = [
    RateLimit(limit_id=CHECK_NETWORK_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=GET_TRADING_PAIRS_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=GET_TRADING_RULES_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=GET_LAST_TRADING_PRICES_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=GET_ORDER_BOOK_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=CREATE_ORDER_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=CANCEL_ORDER_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=GET_ACCOUNT_SUMMARY_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=GET_ORDER_DETAIL_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=GET_TRADE_DETAIL_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=GET_OPEN_ORDERS_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=WS_CONNECT, limit=1, time_interval=1),
    RateLimit(limit_id=WS_SUBSCRIBE, limit=60, time_interval=600),
]

ORDER_STATUS = {
    1: "FAILED",        # Order failure
    2: "OPEN",          # Placing order
    3: "REJECTED",      # Order failure, Freeze failure
    4: "ACTIVE",        # Order success, Pending for fulfilment
    5: "ACTIVE",        # Partially filled
    6: "FILLED",        # Fully filled
    7: "ACTIVE",        # Canceling
    8: "CANCELED",      # Canceled
    9: "ACTIVE",        # Outstanding (4 and 5)
    10: "COMPLETED"     # 6 and 8
}
