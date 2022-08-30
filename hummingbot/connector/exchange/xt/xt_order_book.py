#!/usr/bin/env python
from typing import (
    Optional,
    Dict,
    Any)
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import (
    OrderBookMessageType
)
from hummingbot.connector.exchange.xt.xt_order_book_message import XtOrderBookMessage


class XtOrderBook(OrderBook):

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict[str, any],
                                       timestamp: float,
                                       metadata: Optional[Dict] = None):
        """
        Creates a snapshot message with the order book snapshot message
        :param msg: json snapshot data from live web socket stream
        :param timestamp: timestamp attached to incoming data
        :param metadata: a dictionary with extra information to add to the snapshot data
        :return: XtOrderBookMessage
        """

        if metadata:
            msg.update(metadata)

        return XtOrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content=msg,
            timestamp=timestamp
        )

    @classmethod
    def diff_message_from_exchange(cls,
                                       msg: Dict[str, any],
                                       timestamp: float,
                                       metadata: Optional[Dict] = None):
        """
        Creates a diff message with the changes in the order book received from the exchange
        :param msg: json snapshot data from live web socket stream
        :param timestamp: timestamp attached to incoming data
        :param metadata: a dictionary with extra information to add to the snapshot data
        :return: XtOrderBookMessage
        """

        if metadata:
            msg.update(metadata)

        return XtOrderBookMessage(
            message_type=OrderBookMessageType.DIFF,
            content=msg,
            timestamp=timestamp
        )

    @classmethod
    def trade_message_from_exchange(cls,
                                    msg: Dict[str, Any],
                                    timestamp: Optional[float] = None,
                                    metadata: Optional[Dict] = None):
        """
        Convert a trade data into standard OrderBookMessage format
        :param record: a trade data from the database
        :return: XtOrderBookMessage
        """

        if metadata:
            msg.update(metadata)

        msg.update({
            "exchange_order_id": str(msg.get("s_t")),
            "trade_type": msg.get("side"),
            "price": msg.get("price"),
            "amount": msg.get("size"),
        })

        return XtOrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content=msg,
            timestamp=timestamp
        )
