from pathlib import Path

from cryptopy.trader.app import BaseApp
from cryptopy.trader.constant import Direction
from cryptopy.trader.object import TickData, BarData, TradeData, OrderData
from cryptopy.trader.utility import BarGenerator, ArrayManager

from .base import APP_NAME
from .engine import StrategyEngine
from .template import StrategyTemplate


class PortfolioStrategyApp(BaseApp):
    """"""

    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "Portfolio Strategy"
    engine_class = StrategyEngine
    widget_name = "PortfolioStrategyManager"
    icon_name = str(app_path.joinpath("ui", "strategy.ico"))
