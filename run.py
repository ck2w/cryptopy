from cryptopy.event import EventEngine
from cryptopy.trader.engine import MainEngine
from cryptopy.trader.ui import MainWindow, create_qapp
from cryptopy_okex import OkexGateway
from cryptopy_portfoliostrategy import PortfolioStrategyApp
from cryptopy_datarecorder import DataRecorderApp

def main():
    qapp = create_qapp()

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(OkexGateway)
    main_engine.add_app(PortfolioStrategyApp)
    main_engine.add_app(DataRecorderApp)
    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()


if __name__ == "__main__":
    main()
