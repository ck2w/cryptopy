from typing import List, Dict
from datetime import datetime
from collections import deque
from time import sleep

from cryptopy.trader.utility import ArrayManager
from cryptopy.trader.object import TickData
from cryptopy_portfoliostrategy import StrategyTemplate, StrategyEngine
import pickle
import xgboost as xgb
from pathlib import Path
import pandas as pd
import sklearn

class QingyuWangTestStrategy(StrategyTemplate):
    """"""

    author = "Qingyu Wang"

    price = 40000
    total_money = 1000

    parameters = [
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
        filename = Path(__file__).with_name('rf_model3.sav')
        # self.model = xgb.Booster()
        # self.model.load_model(filename)
        self.model = pickle.load(open(filename, 'rb'))
        self.spot = list()
        self.futures = list()
        self.order_imb_ratio = list()
        # self.ret_10 = deque('ret_10')
        # self.ret_20 = deque('ret_20')
        # self.ret_60 = deque('ret_60')


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
        if tick.symbol in self.vt_symbols[0]:
            mid_price = (tick.ask_price_1 + tick.bid_price_1 ) / 2
            self.spot.append(mid_price)
            bid_size = tick.bid_volume_1
            ask_size = tick.ask_volume_1
            order_imb_ratio = (bid_size - ask_size) / (bid_size + ask_size)
            self.order_imb_ratio.append(order_imb_ratio)
            if len(self.spot) > 61:
                self.spot = self.spot[-61:]
                self.order_imb_ratio = self.order_imb_ratio[-61:]
        else:
            mid_price = (tick.ask_price_1 + tick.bid_price_1 ) / 2
            self.futures.append(mid_price)
            if len(self.futures) > 61:
                self.futures = self.futures[-61:]
        if len(self.spot) >= 60 and len(self.futures) >= 60:
            spot_df = pd.DataFrame(self.spot)
            futures_df = pd.DataFrame(self.futures)
            sp = (futures_df / spot_df - 1)
            ret_10 = sp.pct_change(10).fillna(0)
            ret_20 = sp.pct_change(20).fillna(0)
            ret_60 = sp.pct_change(60).fillna(0)
            X = pd.concat([ret_10, ret_20, ret_60, pd.DataFrame(self.order_imb_ratio)], axis=1).fillna(0)
            X.columns = ["ret_10", "ret_20", "ret_60", "order_imb_ratio"]
            alpha = self.model.predict(X.iloc[[60]])
            # print(X.iloc[[60]])
            print(alpha)
