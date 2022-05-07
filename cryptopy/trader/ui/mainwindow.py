"""
Implements main window of the trading platform.
"""

import webbrowser
from functools import partial
from importlib import import_module
from typing import Callable, Dict, Tuple

import cryptopy
from cryptopy.event import EventEngine

from .qt import QtCore, QtGui, QtWidgets
from .widget import (
    TickMonitor,
    OrderMonitor,
    TradeMonitor,
    PositionMonitor,
    AccountMonitor,
    LogMonitor,
    ActiveOrderMonitor,
    ConnectDialog,
    ContractManager,
    TradingWidget,
    GlobalDialog
)
from ..engine import MainEngine
from ..utility import get_icon_path, TRADER_DIR


class MainWindow(QtWidgets.QMainWindow):
    """
    Main window of the trading platform.
    """

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super().__init__()

        self.main_engine: MainEngine = main_engine
        self.event_engine: EventEngine = event_engine

        self.window_title: str = f"Crypto Trader - {cryptopy.__version__}   [{TRADER_DIR}]"

        self.widgets: Dict[str, QtWidgets.QWidget] = {}

        self.init_ui()

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle(self.window_title)
        self.init_dock()
        self.init_toolbar()
        self.init_menu()
        self.load_window_setting("custom")

    def init_dock(self) -> None:
        """"""
        self.trading_widget, trading_dock = self.create_dock(
            TradingWidget, "Trading", QtCore.Qt.LeftDockWidgetArea
        )
        tick_widget, tick_dock = self.create_dock(
            TickMonitor, "Tick", QtCore.Qt.RightDockWidgetArea
        )
        order_widget, order_dock = self.create_dock(
            OrderMonitor, "Order", QtCore.Qt.RightDockWidgetArea
        )
        active_widget, active_dock = self.create_dock(
            ActiveOrderMonitor, "Active Order", QtCore.Qt.RightDockWidgetArea
        )
        trade_widget, trade_dock = self.create_dock(
            TradeMonitor, "Trade", QtCore.Qt.RightDockWidgetArea
        )
        log_widget, log_dock = self.create_dock(
            LogMonitor, "Log", QtCore.Qt.BottomDockWidgetArea
        )
        account_widget, account_dock = self.create_dock(
            AccountMonitor, "Account", QtCore.Qt.BottomDockWidgetArea
        )
        position_widget, position_dock = self.create_dock(
            PositionMonitor, "Position", QtCore.Qt.BottomDockWidgetArea
        )

        self.tabifyDockWidget(active_dock, order_dock)

        self.save_window_setting("default")

        tick_widget.itemDoubleClicked.connect(self.trading_widget.update_with_cell)
        position_widget.itemDoubleClicked.connect(self.trading_widget.update_with_cell)

    def init_menu(self) -> None:
        """"""
        bar = self.menuBar()
        bar.setNativeMenuBar(False)     # for mac and linux

        # System menu
        sys_menu = bar.addMenu("System")

        gateway_names = self.main_engine.get_all_gateway_names()
        for name in gateway_names:
            func = partial(self.connect, name)
            self.add_action(
                sys_menu,
                f"Connect {name}",
                get_icon_path(__file__, "connect.ico"),
                func
            )

        sys_menu.addSeparator()

        self.add_action(
            sys_menu,
            "Exit",
            get_icon_path(__file__, "exit.ico"),
            self.close
        )

        # App menu
        app_menu = bar.addMenu("Function")

        all_apps = self.main_engine.get_all_apps()
        for app in all_apps:
            ui_module = import_module(app.app_module + ".ui")
            widget_class = getattr(ui_module, app.widget_name)

            func = partial(self.open_widget, widget_class, app.app_name)

            self.add_action(app_menu, app.display_name, app.icon_name, func, True)

        # Global setting editor
        action = QtWidgets.QAction("Setting", self)
        action.triggered.connect(self.edit_global_setting)
        bar.addAction(action)

        # Help menu
        help_menu = bar.addMenu("Help")

        self.add_action(
            help_menu,
            "Contract",
            get_icon_path(__file__, "contract.ico"),
            partial(self.open_widget, ContractManager, "contract"),
            True
        )

        self.add_action(
            help_menu,
            "Restore",
            get_icon_path(__file__, "restore.ico"),
            self.restore_window_setting
        )

    def init_toolbar(self) -> None:
        """"""
        self.toolbar = QtWidgets.QToolBar(self)
        self.toolbar.setObjectName("Tool Bar")
        self.toolbar.setFloatable(False)
        self.toolbar.setMovable(False)

        # Set button size
        w = 40
        size = QtCore.QSize(w, w)
        self.toolbar.setIconSize(size)

        # Set button spacing
        self.toolbar.layout().setSpacing(10)

        self.addToolBar(QtCore.Qt.LeftToolBarArea, self.toolbar)

    def add_action(
        self,
        menu: QtWidgets.QMenu,
        action_name: str,
        icon_name: str,
        func: Callable,
        toolbar: bool = False
    ) -> None:
        """"""
        icon = QtGui.QIcon(icon_name)

        action = QtWidgets.QAction(action_name, self)
        action.triggered.connect(func)
        action.setIcon(icon)

        menu.addAction(action)

        if toolbar:
            self.toolbar.addAction(action)

    def create_dock(
        self,
        widget_class: QtWidgets.QWidget,
        name: str,
        area: int
    ) -> Tuple[QtWidgets.QWidget, QtWidgets.QDockWidget]:
        """
        Initialize a dock widget.
        """
        widget = widget_class(self.main_engine, self.event_engine)

        dock = QtWidgets.QDockWidget(name)
        dock.setWidget(widget)
        dock.setObjectName(name)
        dock.setFeatures(dock.DockWidgetFloatable | dock.DockWidgetMovable)
        self.addDockWidget(area, dock)
        return widget, dock

    def connect(self, gateway_name: str) -> None:
        """
        Open connect dialog for gateway connection.
        """
        dialog = ConnectDialog(self.main_engine, gateway_name)
        dialog.exec_()

    def closeEvent(self, event: QtGui.QCloseEvent) -> None:
        """
        Call main engine close function before exit.
        """
        reply = QtWidgets.QMessageBox.question(
            self,
            "Exit",
            "Confirm Exit?",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
            QtWidgets.QMessageBox.No,
        )

        if reply == QtWidgets.QMessageBox.Yes:
            for widget in self.widgets.values():
                widget.close()
            self.save_window_setting("custom")

            self.main_engine.close()

            event.accept()
        else:
            event.ignore()

    def open_widget(self, widget_class: QtWidgets.QWidget, name: str) -> None:
        """
        Open contract manager.
        """
        widget = self.widgets.get(name, None)
        if not widget:
            widget = widget_class(self.main_engine, self.event_engine)
            self.widgets[name] = widget

        if isinstance(widget, QtWidgets.QDialog):
            widget.exec_()
        else:
            widget.show()

    def save_window_setting(self, name: str):
        """
        Save current window size and state by trader path and setting name.
        """
        settings = QtCore.QSettings(self.window_title, name)
        settings.setValue("state", self.saveState())
        settings.setValue("geometry", self.saveGeometry())

    def load_window_setting(self, name: str) -> None:
        """
        Load previous window size and state by trader path and setting name.
        """
        settings = QtCore.QSettings(self.window_title, name)
        state = settings.value("state")
        geometry = settings.value("geometry")

        if isinstance(state, QtCore.QByteArray):
            self.restoreState(state)
            self.restoreGeometry(geometry)

    def restore_window_setting(self) -> None:
        """
        Restore window to default setting.
        """
        self.load_window_setting("default")
        self.showMaximized()

    def edit_global_setting(self) -> None:
        """
        """
        dialog = GlobalDialog()
        dialog.exec_()
