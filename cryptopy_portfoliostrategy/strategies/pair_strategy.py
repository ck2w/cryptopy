from typing import List, Dict
from datetime import datetime
from collections import deque, OrderedDict
from time import sleep

import pandas as pd
from cryptopy.trader.utility import ArrayManager
from cryptopy.trader.object import TickData
from cryptopy_portfoliostrategy import StrategyTemplate, StrategyEngine
import pickle
from pathlib import Path
import sklearn
import xgboost as xgb

class PairStrategy(StrategyTemplate):
    """"""

    author = "CK"


    futures_face_size = 0.01  # USDT margined, for SWAP

    open_limit = 1e-3
    xch = 'OKEX'
    trade_size = 0.01
    alpha = 0

    parameters = [
                "open_limit",
                "xch",
                "trade_size",
                "alpha",
            ]

    variables = [
    ]

    def __init__(
        self,
        strategy_engine: StrategyEngine,
        strategy_name: str,
        vt_symbols: List[str],
        setting: dict
    ):
        """"""
        super().__init__(strategy_engine, strategy_name, vt_symbols, setting)

        self.targets: Dict[str, int] = {}
        self.last_tick_time: datetime = None

        # Obtain contract info
        self.ams: Dict[str, ArrayManager] = {}
        for vt_symbol in self.vt_symbols:
            self.ams[vt_symbol] = ArrayManager()
            self.targets[vt_symbol] = 0

        # Load model
        self.write_log("start loading model")

        # xgboost model
        filename = Path(__file__).with_name('xgb_pair.bin')
        self.model = xgb.Booster()
        self.model.load_model(filename)

        self.write_log("end loading model")
        print(self.model)

        # trading variables
        self.symbol1 = vt_symbols[0]  # spot
        self.symbol2 = vt_symbols[1]  # futures
        self.current_timestamp = 0

        self.mid_price = OrderedDict()  # {symbol: price[float]}
        self.price_ratio = OrderedDict() # {lookback: values[float]}
        self.past_returns = OrderedDict() # {lookback: returns[float]}
        self.relative_spread = OrderedDict()  # {symbol: value[float]}
        self.order_imb_ratio = OrderedDict() # {symbol: value[float]}

        self.current_position = OrderedDict()
        self.target_position = OrderedDict()

        self.alpha = 0

        self.look_back = [10, 20, 60]

        # init
        for symbol in [self.symbol1, self.symbol2]:
            self.mid_price[symbol] = 0.0
            self.relative_spread[symbol] = 0.0
            self.order_imb_ratio[symbol] = 0.0
            self.current_position[symbol] = 0
            self.target_position[symbol] = 0

        for lb in self.look_back:
            self.price_ratio[lb] = deque()
            self.past_returns[lb] = 0.0


    def on_init(self):
        """
        Callback when strategy is inited.
        """
        self.write_log("Strategy is initiated.")

    def on_start(self):
        """
        Callback when strategy is started.
        """
        self.write_log("Strategy is started.")

    def on_stop(self):
        """
        Callback when strategy is stopped.
        """
        self.write_log("Strategy is stopped.")

    def on_tick(self, tick: TickData):
        """
        Callback of new tick data update.
        """
        symbol = tick.symbol + '.' + self.xch
        self.current_timestamp = tick.datetime

        # current mid price
        mid_price = (tick.ask_price_1 + tick.bid_price_1) / 2

        # current order imbalance
        bid_size = tick.bid_volume_1
        ask_size = tick.ask_volume_1
        self.relative_spread[symbol] = (tick.ask_price_1 - tick.bid_price_1) / mid_price
        self.order_imb_ratio[symbol] = (bid_size - ask_size) / (bid_size + ask_size)

        # update variables
        self.mid_price[symbol] = mid_price
        if self.mid_price[self.symbol1] <= 0 or self.mid_price[self.symbol2] <= 0:
            return

        self.run_alpha()


    def run_alpha(self):
        price_ratio = self.mid_price[self.symbol1] / self.mid_price[self.symbol2]

        for lb in self.look_back:
            self.price_ratio[lb].append((self.current_timestamp, price_ratio))

            while (self.current_timestamp - self.price_ratio[lb][0][0]).seconds > lb:
                self.price_ratio[lb].popleft()

            self.past_returns[lb] = self.price_ratio[lb][-1][1]/self.price_ratio[lb][0][1] - 1

        feature_values = [list(self.relative_spread.values()) + list(self.order_imb_ratio.values()) + list(self.past_returns.values())]

        # xgboost
        self.alpha = self.model.predict(xgb.DMatrix(feature_values))[0]

        if self.trading == True:
            self.run_strategy1(self.alpha)

    def run_strategy1(self, alpha):
        ## strategy
        if alpha > self.open_limit:
            # ratio increase: long symbol1 , short symbol2
            direction = 1

        elif alpha < -self.open_limit:
            # ratio decrease: short symbol1 , long symbol2
            direction = -1
        else:
            return

        self.target_position[self.symbol1] = self.trade_size * direction
        self.target_position[self.symbol2] = int(-self.trade_size * direction / self.futures_face_size)

        self.target_trade1 = self.target_position[self.symbol1] - self.current_position[self.symbol1]
        self.target_trade2 = self.target_position[self.symbol2] - self.current_position[self.symbol2]

        # update current position
        self.current_position[self.symbol1] = self.trade_size * direction
        self.current_position[self.symbol2] = int(-self.trade_size * direction / self.futures_face_size)

        # send orders
        self.send_market_order(self.symbol1, self.target_trade1)
        self.send_market_order(self.symbol2, self.target_trade2)

    def send_market_order(self, symbol, target_trade):
        # send 100 higher buy order or lower sell order to make sure orders match

        if target_trade == 0:
            return
        elif target_trade > 0:
            self.write_log(f'{self.current_timestamp} alpha={self.alpha}')
            self.write_log(f'{symbol} buy {target_trade}')
            self.buy(symbol, self.mid_price[symbol] + 100, abs(target_trade))
        elif target_trade < 0:
            self.write_log(f'{symbol} sell {target_trade}')
            self.sell(symbol, self.mid_price[symbol] - 100, abs(target_trade))

