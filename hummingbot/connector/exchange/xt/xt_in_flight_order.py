from decimal import Decimal
from typing import (
    Any,
    Dict,
    Optional,
    Tuple,
)
import asyncio
from hummingbot.core.event.events import (
    OrderType,
    TradeType
)
from hummingbot.connector.in_flight_order_base import InFlightOrderBase
from hummingbot.connector.exchange.xt import xt_utils


class XtInFlightOrder(InFlightOrderBase):
    def __init__(self,
                 client_order_id: str,
                 exchange_order_id: Optional[str],
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 initial_state: str = "OPEN"):
        super().__init__(
            client_order_id,
            exchange_order_id,
            trading_pair,
            order_type,
            trade_type,
            price,
            amount,
            initial_state,
        )
        self.trade_id_set = set()
        self.cancelled_event = asyncio.Event()

    @property
    def is_done(self) -> bool:
        return self.last_state in {"FILLED", "CANCELED", "REJECTED", "EXPIRED", "FAILED"}

    @property
    def is_failure(self) -> bool:
        return self.last_state in {"REJECTED", "FAILED"}

    @property
    def is_cancelled(self) -> bool:
        return self.last_state in {"CANCELED", "EXPIRED"}

    # @property
    # def order_type_description(self) -> str:
    #     """
    #     :return: Order description string . One of ["limit buy" / "limit sell" / "market buy" / "market sell"]
    #     """
    #     order_type = "market" if self.order_type is OrderType.MARKET else "limit"
    #     side = "buy" if self.trade_type == TradeType.BUY else "sell"
    #     return f"{order_type} {side}"

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> InFlightOrderBase:
        """
        :param data: json data from API
        :return: formatted InFlightOrder
        """
        retval = XtInFlightOrder(
            data["client_order_id"],
            data["exchange_order_id"],
            data["trading_pair"],
            getattr(OrderType, data["order_type"]),
            getattr(TradeType, data["trade_type"]),
            Decimal(data["price"]),
            Decimal(data["amount"]),
            data["last_state"]
        )
        retval.executed_amount_base = Decimal(data["executed_amount_base"])
        retval.executed_amount_quote = Decimal(data["executed_amount_quote"])
        retval.fee_asset = data["fee_asset"]
        retval.fee_paid = Decimal(data["fee_paid"])
        retval.last_state = data["last_state"]
        return retval

    def update_with_order_status(self, order_update: Dict[str, Any]) -> Tuple[Decimal, Decimal, str]:
        """
        Updates the in flight order with order message from order-status REST API
        :return: tuple of trade amount, trade price, trade fee, trade ID if the order gets updated
        """
        s_decimal_0 = Decimal(0)

        if Decimal(order_update["completeNumber"]) <= self.executed_amount_base:
            return (s_decimal_0, s_decimal_0, s_decimal_0, "")

        trade_id = f"{order_update['id']}-{int(time.time() * 1000)}"
        self.trade_id_set.add(trade_id)

        executed_amount_base = Decimal(order_update["completeNumber"])
        executed_amount_quote = abs(Decimal(order_update["completeMoney"]))
        fee_paid = Decimal(order_update["fee"])
        delta_trade_amount = executed_amount_base - self.executed_amount_base
        delta_trade_price = Decimal(order_update["avgPrice"])
        self.executed_amount_base = executed_amount_base
        self.executed_amount_quote = executed_amount_quote
        delta_trade_fee = fee_paid - self.fee_paid
        self.fee_paid = fee_paid

        if not self.fee_asset:
            self.fee_asset = self.quote_asset

        return (delta_trade_amount, delta_trade_price, delta_trade_fee, trade_id)

    def update_with_trade_status(self, trade_update: Dict[str, Any]) -> Tuple[Decimal, Decimal, str]:
        """
        Updates the in flight order with trade message from trade-status REST API
        :return: tuple of trade amount, trade price, trade fee, trade ID if the order gets updated
        """
        s_decimal_0 = Decimal(0)

        trade_id = str(trade_update["id"])
        if trade_id in self.trade_id_set:
            return (s_decimal_0, s_decimal_0, s_decimal_0, "")

        self.trade_id_set.add(trade_id)

        delta_trade_amount = Decimal(trade_update["amount"])
        delta_trade_price = Decimal(str(trade_update["price"]))
        delta_trade_fee = Decimal(str(trade_update["fee"]))
        self.executed_amount_base += delta_trade_amount
        self.executed_amount_quote += abs(Decimal(str(trade_update["value"])))
        self.fee_paid += delta_trade_fee

        if not self.fee_asset:
            self.fee_asset = self.quote_asset

        return (delta_trade_amount, delta_trade_price, delta_trade_fee, trade_id)
