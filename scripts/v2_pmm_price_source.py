import logging
import os
from decimal import Decimal
from enum import Enum
from typing import Dict, List

from pydantic import Field, validator

from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PositionMode, PriceType, TradeType
from hummingbot.data_feed.candles_feed.candles_factory import CandlesConfig
from hummingbot.data_feed.market_data_provider import MarketDataProvider
from hummingbot.logger import HummingbotLogger
from hummingbot.smart_components.executors.position_executor.data_types import (
    PositionExecutorConfig,
    TripleBarrierConfig,
)
from hummingbot.smart_components.models.base import SmartComponentStatus
from hummingbot.smart_components.models.executor_actions import (
    CreateExecutorAction,
    ExecutorAction,
    StopExecutorAction,
    StoreExecutorAction,
)
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase

lsb_logger = None


class PriceAggregatorAlgorithm(Enum):
    MEAN = "mean"


class PMMWithPositionExecutorConfig(StrategyV2ConfigBase):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    candles_config: List[CandlesConfig] = []
    price_source_exchanges: List[str] = Field(
        default="binance",
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the exchanges to use as price source in format 'exchange1,exchange2' (e.g. binance,kraken): ",
            prompt_on_new=True))
    price_source_type: str = Field(
        default="mid_price",
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the price type to use as reference from aggregated from the source exchanges (mid_price/best_bid/best_ask/last_price): ",
            prompt_on_new=True))
    price_aggregator_algorithm: PriceAggregatorAlgorithm = Field(
        default="mean",
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the price aggregator algorithm (mean): ",
            prompt_on_new=True))
    order_amount_quote: Decimal = Field(
        default=30, gt=0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the amount of quote asset to be used per order (e.g. 30): ",
            prompt_on_new=True))
    executor_refresh_time: int = Field(
        default=20, gt=0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the time in seconds to refresh the executor (e.g. 20): ",
            prompt_on_new=True))
    closed_executors_buffer: int = Field(
        default=10, gt=0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the number of closed executors to keep in the buffer (e.g. 10): ",
            prompt_on_new=True))
    spread: Decimal = Field(
        default=Decimal("0.003"), gt=0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the spread (e.g. 0.003): ",
            prompt_on_new=True))
    leverage: int = Field(
        default=20, gt=0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the leverage (e.g. 20): ",
            prompt_on_new=True))
    position_mode: PositionMode = Field(
        default="HEDGE",
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the position mode (HEDGE/ONEWAY): ",
            prompt_on_new=True
        )
    )
    # Triple Barrier Configuration
    stop_loss: Decimal = Field(
        default=Decimal("0.03"), gt=0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the stop loss (as a decimal, e.g., 0.03 for 3%): ",
            prompt_on_new=True))
    take_profit: Decimal = Field(
        default=Decimal("0.01"), gt=0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the take profit (as a decimal, e.g., 0.01 for 1%): ",
            prompt_on_new=True))
    time_limit: int = Field(
        default=60 * 45, gt=0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the time limit in seconds (e.g., 2700 for 45 minutes): ",
            prompt_on_new=True))
    take_profit_order_type: OrderType = Field(
        default="LIMIT",
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the order type for taking profit (LIMIT/MARKET): ",
            prompt_on_new=True))

    @validator('price_source_exchanges', pre=True, allow_reuse=True)
    def parse_price_source_exchanges(cls, v) -> List[str]:
        if isinstance(v, str):
            return [exchange.strip() for exchange in v.split(",")]
        elif isinstance(v, list):
            return v

        raise ValueError("Invalid type for price_source_exchanges. Expected str or List[str]")

    @validator('price_aggregator_algorithm', pre=True, allow_reuse=True)
    def parse_price_aggregator_algorithm(cls, v: str) -> PriceAggregatorAlgorithm:
        if v.upper() in PriceAggregatorAlgorithm.__members__:
            return PriceAggregatorAlgorithm[v.upper()]

        raise ValueError(f"Invalid price aggregator algorithm: {v}. Valid options are: {', '.join(PriceAggregatorAlgorithm.__members__)}")

    @validator('take_profit_order_type', pre=True, allow_reuse=True)
    def validate_order_type(cls, v) -> OrderType:
        if isinstance(v, OrderType):
            return v
        elif isinstance(v, str):
            if v.upper() in OrderType.__members__:
                return OrderType[v.upper()]
        elif isinstance(v, int):
            try:
                return OrderType(v)
            except ValueError:
                pass
        raise ValueError(f"Invalid order type: {v}. Valid options are: {', '.join(OrderType.__members__)}")

    @property
    def triple_barrier_config(self) -> TripleBarrierConfig:
        return TripleBarrierConfig(
            stop_loss=self.stop_loss,
            take_profit=self.take_profit,
            time_limit=self.time_limit,
            open_order_type=OrderType.LIMIT,
            take_profit_order_type=self.take_profit_order_type,
            stop_loss_order_type=OrderType.MARKET,  # Defaulting to MARKET as per requirement
            time_limit_order_type=OrderType.MARKET  # Defaulting to MARKET as per requirement
        )

    @validator('position_mode', pre=True, allow_reuse=True)
    def validate_position_mode(cls, v: str) -> PositionMode:
        if v.upper() in PositionMode.__members__:
            return PositionMode[v.upper()]
        raise ValueError(f"Invalid position mode: {v}. Valid options are: {', '.join(PositionMode.__members__)}")


class PriceAggregator:
    def __init__(self, price_source_exchanges: List[str], trading_pair: str, market_data_provider: MarketDataProvider):
        self.price_source_exchanges = price_source_exchanges
        self.trading_pair = trading_pair
        self.market_data_provider = market_data_provider

    def get_price(self, price_type) -> Decimal:
        ref_prices = []

        for connector_name in self.price_source_exchanges:
            ref_price = self.market_data_provider.get_price_by_type(connector_name, self.trading_pair, price_type)
            ref_prices.append(ref_price)

        return Decimal(sum(ref_prices) / len(ref_prices))


class PMMPriceSource(StrategyV2Base):
    account_config_set = False
    closed_executors_buffer: int = 10  # Number of closed executors to keep in the buffer

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global lsb_logger
        if lsb_logger is None:
            lsb_logger = logging.getLogger(__name__)
        return lsb_logger

    @classmethod
    def init_markets(cls, config: PMMWithPositionExecutorConfig):
        markets = config.markets
        trading_pairs = []
        for connecttor_name in markets.keys():
            trading_pairs.extend(markets[connecttor_name])

        all_trading_pairs = list(set(trading_pairs))

        for connector_name in config.price_source_exchanges:
            markets[connector_name] = all_trading_pairs

        cls.markets = markets

    def __init__(self, connectors: Dict[str, ConnectorBase], config: PMMWithPositionExecutorConfig):
        super().__init__(connectors, config)
        self.config = config  # Only for type checking

        # Initialize price aggregators for each trading pair
        self.price_aggregator: Dict[str, PriceAggregator] = {}

        trading_pairs = []
        for connector_name in self.connectors:
            trading_pairs.extend(self.market_data_provider.get_trading_pairs(connector_name))

        trading_pairs = list(set(trading_pairs))
        for trading_pair in trading_pairs:
            self.price_aggregator[trading_pair] = PriceAggregator(self.config.price_source_exchanges, trading_pair, self.market_data_provider)

    def on_tick(self):
        self.set_position_mode_and_leverage()
        self.update_executors_info()
        if self.market_data_provider.ready:
            executor_actions: List[ExecutorAction] = self.determine_executor_actions()
            for action in executor_actions:
                self.executor_orchestrator.execute_action(action)

    def get_price_type_from_config(self, price_type: str) -> PriceType:
        if price_type == "mid_price":
            return PriceType.MidPrice
        elif price_type == "best_bid":
            return PriceType.BestBid
        elif price_type == "best_ask":
            return PriceType.BestAsk
        elif price_type == "last_price":
            return PriceType.LastTrade

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        """
        Create actions proposal based on the current state of the executors.
        """
        create_actions = []

        all_executors = self.get_all_executors()
        active_buy_position_executors = self.filter_executors(
            executors=all_executors,
            filter_func=lambda x: x.side == TradeType.BUY and x.type == "position_executor" and x.is_active)

        active_sell_position_executors = self.filter_executors(
            executors=all_executors,
            filter_func=lambda x: x.side == TradeType.SELL and x.type == "position_executor" and x.is_active)

        for connector_name in self.connectors:
            for trading_pair in self.market_data_provider.get_trading_pairs(connector_name):
                # Get the reference price from the price aggregator
                price_type = self.get_price_type_from_config(self.config.price_source_type)
                referece_price = self.price_aggregator[trading_pair].get_price(price_type)

                # print(f"Reference Price for {trading_pair} on {connector_name}: {referece_price}")
                self.logger().info(f"Reference Price for {trading_pair} on {connector_name}: {referece_price}")

                len_active_buys = len(self.filter_executors(
                    executors=active_buy_position_executors,
                    filter_func=lambda x: x.config.trading_pair == trading_pair))
                # Evaluate if we need to create new executors and create the actions
                if len_active_buys == 0:
                    order_price = referece_price * (1 - self.config.spread)
                    order_amount = self.config.order_amount_quote / order_price
                    create_actions.append(CreateExecutorAction(
                        executor_config=PositionExecutorConfig(
                            timestamp=self.current_timestamp,
                            trading_pair=trading_pair,
                            exchange=connector_name,
                            side=TradeType.BUY,
                            amount=order_amount,
                            entry_price=order_price,
                            triple_barrier_config=self.config.triple_barrier_config,
                            leverage=self.config.leverage
                        )
                    ))
                len_active_sells = len(self.filter_executors(
                    executors=active_sell_position_executors,
                    filter_func=lambda x: x.config.trading_pair == trading_pair))
                if len_active_sells == 0:
                    order_price = referece_price * (1 + self.config.spread)
                    order_amount = self.config.order_amount_quote / order_price
                    create_actions.append(CreateExecutorAction(
                        executor_config=PositionExecutorConfig(
                            timestamp=self.current_timestamp,
                            trading_pair=trading_pair,
                            exchange=connector_name,
                            side=TradeType.SELL,
                            amount=order_amount,
                            entry_price=order_price,
                            triple_barrier_config=self.config.triple_barrier_config,
                            leverage=self.config.leverage
                        )
                    ))
        return create_actions

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        """
        Create a list of actions to stop the executors based on order refresh and early stop conditions.
        """
        stop_actions = []
        stop_actions.extend(self.executors_to_refresh())
        stop_actions.extend(self.executors_to_early_stop())
        return stop_actions

    def store_actions_proposal(self) -> List[StoreExecutorAction]:
        """
        Create a list of actions to store the executors that have been stopped.
        """
        potential_executors_to_store = self.filter_executors(
            executors=self.get_all_executors(),
            filter_func=lambda x: x.status == SmartComponentStatus.TERMINATED and x.type == "position_executor")
        sorted_executors = sorted(potential_executors_to_store, key=lambda x: x.timestamp, reverse=True)
        if len(sorted_executors) > self.closed_executors_buffer:
            return [StoreExecutorAction(executor_id=executor.id) for executor in
                    sorted_executors[self.closed_executors_buffer:]]
        return []

    def executors_to_refresh(self) -> List[StopExecutorAction]:
        """
        Create a list of actions to stop the executors that need to be refreshed.
        """
        all_executors = self.get_all_executors()
        executors_to_refresh = self.filter_executors(
            executors=all_executors,
            filter_func=lambda x: not x.is_trading and x.is_active and self.current_timestamp - x.timestamp > self.config.executor_refresh_time)

        return [StopExecutorAction(executor_id=executor.id) for executor in executors_to_refresh]

    def executors_to_early_stop(self) -> List[StopExecutorAction]:
        """
        Create a list of actions to stop the executors that need to be early stopped based on signals.
        This is a simple example, in a real strategy you would use signals from the market data provider.
        """
        return []

    def set_position_mode_and_leverage(self):
        if not self.account_config_set:
            for connector_name, connector in self.connectors.items():
                if self.is_perpetual(connector_name):
                    connector.set_position_mode(self.config.position_mode)
                    for trading_pair in self.market_data_provider.get_trading_pairs(connector_name):
                        connector.set_leverage(trading_pair, self.config.leverage)
            self.account_config_set = True
