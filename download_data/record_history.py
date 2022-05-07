
import time
from pytz import timezone
from datetime import datetime
import requests
import pandas as pd

from cryptopy.trader.constant import Exchange, Interval
from cryptopy.trader.object import HistoryRequest, BarData


NY_TZ: timezone = timezone("America/New_York")

def parse_timestamp(timestamp: str) -> datetime:
    dt: datetime = datetime.fromtimestamp(int(timestamp) / 1000)
    return NY_TZ.localize(dt)

def query_hist_data(req):
    buf: Dict[datetime, BarData] = {}
    if req.end:
        end_time = str(int(req.end.timestamp() * 1000))
    else:
        end_time = ""

    start_time = str(int(req.start.timestamp() * 1000))

    path: str = "/api/v5/market/history-candles"

    while True:
        params: dict = {
            "instId": req.symbol,
        }

        if end_time:
            params["after"] = end_time
        r = requests.get('https://www.okx.com/api/v5/market/history-candles',
                         params={'instId': req.symbol, 'after': end_time, 'bar': '1m'})

        data: dict = r.json()

        if 'data' not in data:
            m = data["msg"]
            msg = f"Historical data empty, {m}"
            break

        for bar_list in data["data"]:
            ts, o, h, l, c, vol, _ = bar_list
            dt = parse_timestamp(ts)
            bar: BarData = BarData(
                symbol=req.symbol,
                exchange=req.exchange,
                datetime=dt,
                interval=req.interval,
                volume=float(vol),
                open_price=float(o),
                high_price=float(h),
                low_price=float(l),
                close_price=float(c),
                gateway_name='OKX',
            )
            buf[bar.datetime] = bar

        begin: str = data["data"][-1][0]
        end: str = data["data"][0][0]
        msg: str = f"Fetch data success, {req.symbol} - {req.interval.value}，{parse_timestamp(begin)} - {parse_timestamp(end)}"
        print(msg)

        end_time = begin
        if end_time < start_time:
            break

    index: List[datetime] = list(buf.keys())
    index.sort()

    history: List[BarData] = [buf[i] for i in index]
    return history


def run_task():

    start_time = datetime(2020, 1, 1)
    end_time = datetime(2022, 4, 22)

    req = HistoryRequest(
        symbol="BTC-USDT",
        exchange=Exchange.OKEX,
        start=start_time,
        end=end_time,
        interval=Interval.MINUTE,
    )

    data = query_hist_data(req)

    data_df = pd.DataFrame([[bar.datetime, bar.symbol,
                             bar.open_price, bar.high_price, bar.low_price, bar.close_price,
                             bar.volume] for bar in data],
                            columns=['time', 'symbol', 'open', 'high', 'low', 'close', 'volume'])
    data_df = data_df.drop_duplicates()
    data_df = data_df[(data_df['time'] >= pd.Timestamp(start_time, tz="America/New_York")) & (data_df['time'] <= pd.Timestamp(end_time, tz="America/New_York"))]
    return data_df

if __name__ == "__main__":
    t1 = time.time()
    data_df = run_task()
    t2 = time.time()
    print(data_df.shape)
    print(t2 - t1)
    data_df.to_csv('bar_0.csv')

    # single-thread
    # (1212300, 7)
    # 3846.85s
