from decimal import Decimal
import pandas as pd
from typing import Dict, List, Optional
import unittest.mock

from hummingbot.core.clock import Clock, ClockMode
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import MarketEvent, OrderBookTradeEvent, TradeType
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.liquidity_mining.data_types import PriceSize, Proposal
from hummingbot.strategy.liquidity_mining.liquidity_mining import LiquidityMiningStrategy

from hummingsim.backtest.backtest_market import BacktestMarket
from hummingsim.backtest.market import QuantizationParams
from hummingsim.backtest.mock_order_book_loader import MockOrderBookLoader
from hummingbot.core.event.events import TradeFee


class LiquidityMiningTest(unittest.TestCase):
    start: pd.Timestamp = pd.Timestamp("2019-01-01", tz="UTC")
    end: pd.Timestamp = pd.Timestamp("2019-01-01 01:00:00", tz="UTC")
    start_timestamp: float = start.timestamp()
    end_timestamp: float = end.timestamp()
    market_infos: Dict[str, MarketTradingPairTuple] = {}

    @staticmethod
    def create_market(trading_pairs: List[str], mid_price, balances: Dict[str, int]) -> (BacktestMarket, Dict[str, MarketTradingPairTuple]):
        """
        Create a BacktestMarket and marketinfo dictionary to be used by the liquidity mining strategy
        """
        market: BacktestMarket = BacktestMarket()
        market_infos: Dict[str, MarketTradingPairTuple] = {}

        for trading_pair in trading_pairs:
            base_asset = trading_pair.split("-")[0]
            quote_asset = trading_pair.split("-")[1]

            book_data: MockOrderBookLoader = MockOrderBookLoader(trading_pair, base_asset, quote_asset)
            book_data.set_balanced_order_book(mid_price=mid_price,
                                              min_price=1,
                                              max_price=200,
                                              price_step_size=1,
                                              volume_step_size=10)
            market.add_data(book_data)
            market.set_quantization_param(QuantizationParams(trading_pair, 6, 6, 6, 6))
            market_infos[trading_pair] = MarketTradingPairTuple(market, trading_pair, base_asset, quote_asset)

        for asset, value in balances.items():
            market.set_balance(asset, value)

        return market, market_infos

    def setUp(self) -> None:
        self.clock_tick_size = 1
        self.clock: Clock = Clock(ClockMode.BACKTEST, self.clock_tick_size, self.start_timestamp, self.end_timestamp)

        self.mid_price = 100
        self.bid_spread = 0.01
        self.ask_spread = 0.01
        self.order_refresh_time = 1

        trading_pairs = list(map(lambda quote_asset: "ETH-" + quote_asset, ["USDT", "BTC"]))
        market, market_infos = self.create_market(trading_pairs, self.mid_price, {"USDT": 5000, "ETH": 500, "BTC": 100})
        self.market = market
        self.market_infos = market_infos

        self.clock.add_iterator(self.market)
        self.order_fill_logger: EventLogger = EventLogger()
        self.cancel_order_logger: EventLogger = EventLogger()
        self.market.add_listener(MarketEvent.OrderFilled, self.order_fill_logger)
        self.market.add_listener(MarketEvent.OrderCancelled, self.cancel_order_logger)

        self.default_strategy = LiquidityMiningStrategy(
            exchange=self.market,
            market_infos=self.market_infos,
            token="ETH",
            order_amount=Decimal(2),
            spread=Decimal(0.0005),
            inventory_skew_enabled=False,
            target_base_pct=Decimal(0.5),
            order_refresh_time=5,
            order_refresh_tolerance_pct=Decimal(0.1),  # tolerance of 10 % change
            max_order_age=3,
        )

    def simulate_maker_market_trade(
            self, is_buy: bool, quantity: Decimal, price: Decimal, trading_pair: str, market: Optional[BacktestMarket] = None,
    ):
        """
        simulate making a trade, broadcasts a trade event
        """
        if market is None:
            market = self.market
        order_book: OrderBook = market.get_order_book(trading_pair)
        trade_event = OrderBookTradeEvent(
            trading_pair,
            self.clock.current_timestamp,
            TradeType.BUY if is_buy else TradeType.SELL,
            price,
            quantity
        )
        order_book.apply_trade(trade_event)

    @staticmethod
    def has_limit_order_type(limit_orders: List[LimitOrder], trading_pair: str, is_buy: bool) -> bool:
        for limit_order in limit_orders:
            if limit_order.trading_pair == trading_pair and limit_order.is_buy == is_buy:
                return True
        return False

    @unittest.mock.patch('hummingbot.strategy.liquidity_mining.liquidity_mining.estimate_fee')
    def test_simulate_maker_market_trade(self, estimate_fee_mock):
        """
        Test that we can set up a liquidity mining strategy, and a trade
        """
        estimate_fee_mock.return_value = TradeFee(percent=0, flat_fees=[('ETH', Decimal(0.00005))])

        # initiate
        self.clock.add_iterator(self.default_strategy)
        self.clock.backtest_til(self.start_timestamp)

        # assert that there are no active trades on initialization and before clock has moved forward
        self.assertEqual(0, len(self.default_strategy.active_orders))

        # advance by one tick, the strategy will initiate two orders per pair
        self.clock.backtest_til(self.start_timestamp + self.clock_tick_size)
        self.assertEqual(4, len(self.default_strategy.active_orders))

        # Simulate buy order fill
        self.clock.backtest_til(self.start_timestamp + 8)
        self.simulate_maker_market_trade(False, 50, 1, "ETH-USDT")
        self.assertEqual(3, len(self.default_strategy.active_orders))

    @unittest.mock.patch('hummingbot.strategy.liquidity_mining.liquidity_mining.estimate_fee')
    def test_multiple_markets(self, estimate_fee_mock):
        """
        Liquidity Mining supports one base asset but multiple quote assets. This shows that the user can successfully
        provide liquidity for two different pairs and the market can execute the other side of them.
        """
        estimate_fee_mock.return_value = TradeFee(percent=0, flat_fees=[('ETH', Decimal(0.00005))])

        # initiate
        self.clock.add_iterator(self.default_strategy)
        self.clock.backtest_til(self.start_timestamp + self.clock_tick_size)

        # ETH-USDT
        self.simulate_maker_market_trade(False, 50, 1, "ETH-USDT")
        self.clock.backtest_til(self.start_timestamp + 8)

        # ETH-BTC
        self.simulate_maker_market_trade(False, 50, 1, "ETH-BTC")
        self.clock.backtest_til(self.start_timestamp + 16)

    def test_order_refresh_time(self):
        """
        Assert that orders are refreshed within the expected time
        """
        pass

    @unittest.mock.patch('hummingbot.strategy.liquidity_mining.liquidity_mining.estimate_fee')
    def test_tolerance_level(self, estimate_fee_mock):
        """
        Test tolerance level
        """
        estimate_fee_mock.return_value = TradeFee(percent=0, flat_fees=[('ETH', Decimal(0.00005))])

        # initiate strategy and add active orders
        self.clock.add_iterator(self.default_strategy)
        self.clock.backtest_til(self.start_timestamp + 10)

        # the order tolerance is 1%
        # set the orders to the same values
        proposal = Proposal("ETH-USDT", PriceSize(100, 1), PriceSize(100, 1))
        self.assertTrue(self.default_strategy.is_within_tolerance(self.default_strategy.active_orders, proposal))

        # update orders to withint the tolerance
        proposal = Proposal("ETH-USDT", PriceSize(109, 1), PriceSize(91, 1))
        self.assertTrue(self.default_strategy.is_within_tolerance(self.default_strategy.active_orders, proposal))

        # push the orders beyond the tolerance, this proposal should return False
        proposal = Proposal("ETH-USDT", PriceSize(150, 1), PriceSize(50, 1))
        self.assertFalse(self.default_strategy.is_within_tolerance(self.default_strategy.active_orders, proposal))

    @unittest.mock.patch('hummingbot.strategy.liquidity_mining.liquidity_mining.estimate_fee')
    def test_budget_allocation(self, estimate_fee_mock):
        """
        Liquidity mining strategy budget allocation is different from pmm, it depends on the token base and it splits
        its budget between the quote tokens.
        """
        estimate_fee_mock.return_value = TradeFee(percent=0, flat_fees=[('ETH', Decimal(0.00005))])

        # initiate
        usdt_balance = 1000
        busd_balance = 900
        eth_balance = 100
        btc_balance = 10

        trading_pairs = list(map(lambda quote_asset: "ETH-" + quote_asset, ["USDT", "BUSD", "BTC"]))
        market, market_infos = self.create_market(trading_pairs, 100, {"USDT": usdt_balance, "BUSD": busd_balance, "ETH": eth_balance, "BTC": btc_balance})

        strategy = LiquidityMiningStrategy(
            exchange=market,
            market_infos=market_infos,
            token="ETH",
            order_amount=Decimal(2),
            spread=Decimal(0.0005),
            inventory_skew_enabled=False,
            target_base_pct=Decimal(0.5),
            order_refresh_time=5,
            order_refresh_tolerance_pct=Decimal(0.1),  # tolerance of 10 % change
        )

        self.clock.add_iterator(strategy)
        self.clock.backtest_til(self.start_timestamp + 10)

        # there should be a buy and sell budget for each pair
        self.assertEqual(len(strategy.sell_budgets), 3)
        self.assertEqual(len(strategy.buy_budgets), 3)

        # the buy budgets use all of the available balance for the quote tokens
        self.assertEqual(strategy.buy_budgets["ETH-USDT"], usdt_balance)
        self.assertEqual(strategy.buy_budgets["ETH-BTC"], btc_balance)
        self.assertEqual(strategy.buy_budgets["ETH-BUSD"], busd_balance)

        # the sell budget tries to evenly split the base token between the quote tokens
        self.assertLess(strategy.sell_budgets["ETH-USDT"], eth_balance * 0.4)
        self.assertLess(strategy.sell_budgets["ETH-BTC"], eth_balance * 0.4)
        self.assertLess(strategy.sell_budgets["ETH-BUSD"], eth_balance * 0.4)

    @unittest.mock.patch('hummingbot.strategy.liquidity_mining.liquidity_mining.estimate_fee')
    def test_inventory_skew(self, estimate_fee_mock):
        """
        When inventory_skew_enabled is true, the strategy will try to balance the amounts of base to match it
        """
        estimate_fee_mock.return_value = TradeFee(percent=0, flat_fees=[('ETH', Decimal(0.00005))])

        # initiate with similar balances so the skew is obvious
        usdt_balance = 1000
        busd_balance = 1000
        eth_balance = 1000
        btc_balance = 1000

        trading_pairs = list(map(lambda quote_asset: "ETH-" + quote_asset, ["USDT", "BUSD", "BTC"]))
        market, market_infos = self.create_market(trading_pairs, 100, {"USDT": usdt_balance, "BUSD": busd_balance, "ETH": eth_balance, "BTC": btc_balance})

        skewed_base_strategy = LiquidityMiningStrategy(
            exchange=market,
            market_infos=market_infos,
            token="ETH",
            order_amount=Decimal(2),
            spread=Decimal(0.0005),
            inventory_skew_enabled=True,
            target_base_pct=Decimal(0.1),  # less base, more quote
            order_refresh_time=5,
            order_refresh_tolerance_pct=Decimal(0.1),  # tolerance of 10 % change
        )

        unskewed_strategy = LiquidityMiningStrategy(
            exchange=market,
            market_infos=market_infos,
            token="ETH",
            order_amount=Decimal(2),
            spread=Decimal(0.0005),
            inventory_skew_enabled=False,
            order_refresh_time=5,
            target_base_pct=Decimal(0.1),  # this does nothing when inventory_skew_enabled is False
            order_refresh_tolerance_pct=Decimal(0.1),  # tolerance of 10 % change
        )

        self.clock.add_iterator(skewed_base_strategy)
        self.clock.backtest_til(self.start_timestamp + 10)
        self.clock.add_iterator(unskewed_strategy)
        self.clock.backtest_til(self.start_timestamp + 20)

        # iterate through pairs in skewed and unskewed strategy
        for unskewed_order in unskewed_strategy.active_orders:
            for skewed_base_order in skewed_base_strategy.active_orders:
                # if the trading_pair and trade type are the same, compare them
                if skewed_base_order.trading_pair == unskewed_order.trading_pair and \
                        skewed_base_order.is_buy == unskewed_order.is_buy:
                    if skewed_base_order.is_buy:
                        # the skewed strategy tries to buy more quote thant the unskewed one
                        self.assertGreater(skewed_base_order.price, unskewed_order.price)
                    else:
                        # trying to keep less base
                        self.assertLessEqual(skewed_base_order.price, unskewed_order.price)

    @unittest.mock.patch('hummingbot.strategy.liquidity_mining.liquidity_mining.MarketTradingPairTuple.get_mid_price')
    @unittest.mock.patch('hummingbot.strategy.liquidity_mining.liquidity_mining.estimate_fee')
    def test_volatility(self, estimate_fee_mock, get_mid_price_mock):
        """
        Assert that volatility information is updated after the expected number of intervals
        """
        estimate_fee_mock.return_value = TradeFee(percent=0, flat_fees=[('ETH', Decimal(0.00005))])

        # initiate with similar balances so the skew is obvious
        usdt_balance = 1000
        eth_balance = 1000

        trading_pairs = list(map(lambda quote_asset: "ETH-" + quote_asset, ["USDT"]))
        market, market_infos = self.create_market(trading_pairs, 100, {"USDT": usdt_balance, "ETH": eth_balance})

        strategy = LiquidityMiningStrategy(
            exchange=market,
            market_infos=market_infos,
            token="ETH",
            order_amount=Decimal(2),
            spread=Decimal(0.0005),
            inventory_skew_enabled=False,
            target_base_pct=Decimal(0.5),  # less base, more quote
            order_refresh_time=1,
            order_refresh_tolerance_pct=Decimal(0.1),  # tolerance of 10 % change
            volatility_interval=2,
            avg_volatility_period=2,
            volatility_to_spread_multiplier=2,
        )

        self.clock.add_iterator(strategy)
        self.clock.backtest_til(self.start_timestamp + 1)

        # update prices to create volatility after 2 intervals
        get_mid_price_mock.return_value = Decimal(105.0)
        self.clock.backtest_til(self.start_timestamp + 2)

        get_mid_price_mock.return_value = Decimal(110.0)
        self.clock.backtest_til(self.start_timestamp + 3)

        # assert that volatility is none zero
        self.assertGreater(float(strategy.market_status_df().loc[0, 'Volatility'].strip('%')), 0.00)
