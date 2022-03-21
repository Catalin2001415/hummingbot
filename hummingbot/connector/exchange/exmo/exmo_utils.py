import math
import zlib
from typing import Dict, List, Tuple

from hummingbot.core.utils.tracking_nonce import get_tracking_nonce, get_tracking_nonce_low_res
from hummingbot.core.data_type.order_book_row import OrderBookRow
from hummingbot.core.data_type.order_book_message import OrderBookMessage

from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_methods import using_exchange


CENTRALIZED = True

EXAMPLE_PAIR = "BTC-USD"

DEFAULT_FEES = [0.4, 0.4]

HBOT_BROKER_ID = "exmo-"


# deeply merge two dictionaries
def merge_dicts(source: Dict, destination: Dict) -> Dict:
    for key, value in source.items():
        if isinstance(value, dict):
            # get node or create one
            node = destination.setdefault(key, {})
            merge_dicts(value, node)
        else:
            destination[key] = value

    return destination


# join paths
def join_paths(*paths: List[str]) -> str:
    return "/".join(paths)


# get timestamp in milliseconds
def get_ms_timestamp() -> int:
    return get_tracking_nonce_low_res()


# convert milliseconds timestamp to seconds
def ms_timestamp_to_s(ms: int) -> int:
    return math.floor(ms / 1e3)


def convert_snapshot_message_to_order_book_row(message: OrderBookMessage) -> Tuple[List[OrderBookRow], List[OrderBookRow]]:
    update_id = message.update_id
    data = message.content
    bids, asks = [], []

    bids = [
        OrderBookRow(float(bid[0]), float(bid[1]), update_id) for bid in data["bid"]
    ]
    sorted(bids, key=lambda a: a.price)

    asks = [
        OrderBookRow(float(ask[0]), float(ask[1]), update_id) for ask in data["ask"]
    ]
    sorted(asks, key=lambda a: a.price)

    return bids, asks


def convert_diff_message_to_order_book_row(message: OrderBookMessage) -> Tuple[List[OrderBookRow], List[OrderBookRow]]:
    update_id = message.update_id
    data = message.content
    bids, asks = [], []

    bids = [
        OrderBookRow(float(bid[0]), float(bid[1]), update_id) for bid in data["bid"]
    ]
    sorted(bids, key=lambda a: a.price)

    asks = [
        OrderBookRow(float(ask[0]), float(ask[1]), update_id) for ask in data["ask"]
    ]
    sorted(asks, key=lambda a: a.price)

    return bids, asks


# Nonce class
class ExmoNone:
    """
    Generate unique nonces
    """
    _request_id: int = 0

    @classmethod
    def get_nonce(cls) -> int:
        return get_tracking_nonce()


exmo_nonce = ExmoNone()

def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> str:
    return exchange_trading_pair.replace("_", "-")


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    return hb_trading_pair.replace("-", "_")


def get_new_client_order_id(is_buy: bool, trading_pair: str) -> str:
    return f"{get_tracking_nonce()}"



KEYS = {
    "exmo_api_key":
        ConfigVar(key="exmo_api_key",
                  prompt="Enter your Exmo API key >>> ",
                  required_if=using_exchange("exmo"),
                  is_secure=True,
                  is_connect_key=True),
    "exmo_secret_key":
        ConfigVar(key="exmo_secret_key",
                  prompt="Enter your Exmo secret key >>> ",
                  required_if=using_exchange("exmo"),
                  is_secure=True,
                  is_connect_key=True),
}
