import base64
import hashlib
import hmac
import json
import sys
import time
from copy import copy
from datetime import datetime
from urllib.parse import urlencode
from typing import Dict, List, Set
from types import TracebackType

from requests import Response
from pytz import timezone

from cryptopy.event.engine import EventEngine
from cryptopy.trader.constant import (
    Direction,
    Exchange,
    Interval,
    Offset,
    OrderType,
    Product,
    Status,
    OptionType
)
from cryptopy.trader.gateway import BaseGateway
from cryptopy.trader.utility import round_to
from cryptopy.trader.object import (
    AccountData,
    BarData,
    CancelRequest,
    ContractData,
    HistoryRequest,
    OrderData,
    OrderRequest,
    PositionData,
    SubscribeRequest,
    TickData,
    TradeData
)
from cryptopy_rest import Request, RestClient
from cryptopy_websocket import WebsocketClient


# NY Time Zone
NY_TZ: timezone = timezone("America/New_York")

# REST API Address
REST_HOST: str = "https://www.okex.com"

# Live Trading Websocket API Address
PUBLIC_WEBSOCKET_HOST: str = "wss://ws.okex.com:8443/ws/v5/public"
PRIVATE_WEBSOCKET_HOST: str = "wss://ws.okex.com:8443/ws/v5/private"

# Demo Trading Websocket API Address
TEST_PUBLIC_WEBSOCKET_HOST: str = "wss://wspap.okex.com:8443/ws/v5/public?brokerId=9999"
TEST_PRIVATE_WEBSOCKET_HOST: str = "wss://wspap.okex.com:8443/ws/v5/private?brokerId=9999"

STATUS_OKEX2VT: Dict[str, Status] = {
    "live": Status.NOTTRADED,
    "partially_filled": Status.PARTTRADED,
    "filled": Status.ALLTRADED,
    "canceled": Status.CANCELLED
}

ORDERTYPE_OKEX2VT: Dict[str, OrderType] = {
    "limit": OrderType.LIMIT,
    "fok": OrderType.FOK,
    "ioc": OrderType.FAK,
    "market": OrderType.MARKET,
}
ORDERTYPE_VT2OKEX: Dict[OrderType, str] = {v: k for k, v in ORDERTYPE_OKEX2VT.items()}

DIRECTION_OKEX2VT: Dict[str, Direction] = {
    "buy": Direction.LONG,
    "sell": Direction.SHORT
}
DIRECTION_VT2OKEX: Dict[Direction, str] = {v: k for k, v in DIRECTION_OKEX2VT.items()}

INTERVAL_VT2OKEX: Dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "1H",
    Interval.DAILY: "1D",
}

PRODUCT_OKEX2VT: Dict[str, Product] = {
    "SWAP": Product.FUTURES,
    "SPOT": Product.SPOT,
    "FUTURES": Product.FUTURES,
    "OPTION": Product.OPTION
}
PRODUCT_VT2OKEX: Dict[Product, str] = {v: k for k, v in PRODUCT_OKEX2VT.items()}

OPTIONTYPE_OKEXO2VT: Dict[str, OptionType] = {
    "C": OptionType.CALL,
    "P": OptionType.PUT
}

symbol_contract_map: Dict[str, ContractData] = {}

local_orderids: Set[str] = set()


class OkexGateway(BaseGateway):

    default_setting = {
        "API Key": "",
        "Secret Key": "",
        "Passphrase": "",
        "Sessions": 3,
        "Proxy Address": "",
        "Proxy Port": "",
        "Server": ["REAL", "TEST"]
    }

    exchanges = [Exchange.OKEX]

    def __init__(self, event_engine: EventEngine, gateway_name: str = "OKEX") -> None:
        super().__init__(event_engine, gateway_name)

        self.rest_api: "OkexRestApi" = OkexRestApi(self)
        self.ws_public_api: "OkexWebsocketPublicApi" = OkexWebsocketPublicApi(self)
        self.ws_private_api: "OkexWebsocketPrivateApi" = OkexWebsocketPrivateApi(self)

        self.orders: Dict[str, OrderData] = {}

    def connect(self, setting: dict) -> None:
        key: str = setting["API Key"]
        secret: str = setting["Secret Key"]
        passphrase: str = setting["Passphrase"]
        proxy_host: str = setting["Proxy Address"]
        proxy_port: str = setting["Proxy Port"]
        server: str = setting["Server"]
        session_number: str = setting["Sessions"]

        if proxy_port.isdigit():
            proxy_port = int(proxy_port)
        else:
            proxy_port = 0

        self.rest_api.connect(
            key,
            secret,
            passphrase,
            session_number,
            proxy_host,
            proxy_port,
            server
        )
        self.ws_public_api.connect(
            proxy_host,
            proxy_port,
            server
        )
        self.ws_private_api.connect(
            key,
            secret,
            passphrase,
            proxy_host,
            proxy_port,
            server
        )

    def subscribe(self, req: SubscribeRequest) -> None:
        self.ws_public_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        return self.ws_private_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        self.ws_private_api.cancel_order(req)

    def query_account(self) -> None:
        pass

    def query_position(self) -> None:
        pass

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        return self.rest_api.query_history(req)

    def close(self) -> None:
        self.rest_api.stop()
        self.ws_public_api.stop()
        self.ws_private_api.stop()

    def on_order(self, order: OrderData) -> None:
        self.orders[order.orderid] = order
        super().on_order(order)

    def get_order(self, orderid: str) -> OrderData:
        return self.orders.get(orderid, None)


class OkexRestApi(RestClient):
    """"""

    def __init__(self, gateway: OkexGateway) -> None:
        super().__init__()

        self.gateway: OkexGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: str = ""
        self.passphrase: str = ""
        self.simulated: bool = False

    def sign(self, request: Request) -> Request:
        timestamp: str = generate_timestamp()
        request.data = json.dumps(request.data)

        if request.params:
            path: str = request.path + "?" + urlencode(request.params)
        else:
            path: str = request.path

        msg: str = timestamp + request.method + path + request.data
        signature: bytes = generate_signature(msg, self.secret)
        
        request.headers = {
            "OK-ACCESS-KEY": self.key,
            "OK-ACCESS-SIGN": signature.decode(),
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json"
        }

        if self.simulated:
            request.headers["x-simulated-trading"] = "1"

        return request

    def connect(
        self,
        key: str,
        secret: str,
        passphrase: str,
        session_number: int,
        proxy_host: str,
        proxy_port: int,
        server: str
    ) -> None:
        self.key = key
        self.secret = secret.encode()
        self.passphrase = passphrase

        if server == "TEST":
            self.simulated = True

        self.connect_time = int(datetime.now().strftime("%y%m%d%H%M%S"))

        self.init(REST_HOST, proxy_host, proxy_port)
        self.start(session_number)
        self.gateway.write_log("REST API Start successfully")

        self.query_time()
        self.query_order()

    def query_order(self) -> None:
        self.add_request(
            "GET",
            "/api/v5/trade/orders-pending",
            callback=self.on_query_order,
        )

    def query_time(self) -> None:
        self.add_request(
            "GET",
            "/api/v5/public/time",
            callback=self.on_query_time
        )

    def on_query_order(self, packet: dict, request: Request) -> None:
        for order_info in packet["data"]:
            order: OrderData = parse_order_data(
                order_info,
                self.gateway_name
            )
            self.gateway.on_order(order)


    def on_query_time(self, packet: dict, request: Request) -> None:
        timestamp: int = int(packet["data"][0]["ts"])
        server_time: datetime = datetime.fromtimestamp(timestamp / 1000)
        local_time: datetime = datetime.now()
        msg: str = f"Server time: {server_time}, Local time: {local_time}"
        self.gateway.write_log(msg)

    def on_error(
        self,
        exception_type: type,
        exception_value: Exception,
        tb: TracebackType,
        request: Request
    ) -> None:
        msg: str = f"{exception_type}, msg: {exception_value}"
        self.gateway.write_log(msg)

        sys.stderr.write(
            self.exception_detail(exception_type, exception_value, tb, request)
        )

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        buf: Dict[datetime, BarData] = {}
        end_time: str = ""
        path: str = "/api/v5/market/candles"

        for i in range(15):
            params: dict = {
                "instId": req.symbol,
                "bar": INTERVAL_VT2OKEX[req.interval]
            }

            if end_time:
                params["after"] = end_time

            resp: Response = self.request(
                "GET",
                path,
                params=params
            )

            if resp.status_code // 100 != 2:
                msg = f"Failed to get history data: {resp.status_code}, msg: {resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                data: dict = resp.json()

                if not data["data"]:
                    m = data["msg"]
                    msg = f"Empty history: {m}"
                    break

                for bar_list in data["data"]:
                    ts, o, h, l, c, vol, _ = bar_list
                    dt = parse_timestamp(ts)
                    bar = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=dt,
                        interval=req.interval,
                        volume=float(vol),
                        open_price=float(o),
                        high_price=float(h),
                        low_price=float(l),
                        close_price=float(c),
                        gateway_name=self.gateway_name
                    )
                    buf[bar.datetime] = bar

                begin: str = data["data"][-1][0]
                end: str = data["data"][0][0]
                msg: str = f"Successfully get history data: {req.symbol} - {req.interval.value}, {parse_timestamp(begin)} - {parse_timestamp(end)}"
                self.gateway.write_log(msg)

                end_time = begin

        index: List[datetime] = list(buf.keys())
        index.sort()

        history: List[BarData] = [buf[i] for i in index]
        return history


class OkexWebsocketPublicApi(WebsocketClient):
    """"""

    def __init__(self, gateway: OkexGateway) -> None:
        super().__init__()

        self.gateway: OkexGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.subscribed: Dict[str, SubscribeRequest] = {}
        self.ticks: Dict[str, TickData] = {}

        self.callbacks: Dict[str, callable] = {
            "tickers": self.on_ticker,
            "books5": self.on_depth,
            "instruments": self.on_instrument
        }

    def connect(
        self,
        proxy_host: str,
        proxy_port: int,
        server: str
    ) -> None:
        if server == "REAL":
            self.init(PUBLIC_WEBSOCKET_HOST, proxy_host, proxy_port, 20)
        else:
            self.init(TEST_PUBLIC_WEBSOCKET_HOST, proxy_host, proxy_port, 20)

        self.start()

    def query_contract(self) -> None:
        args: list = []
        for inst_type in ["SPOT", "SWAP", "FUTURES", "OPTION"]:
            args.append({
                "channel": "instruments",
                "instType": inst_type
            })

        okex_req: dict = {
            "op": "subscribe",
            "args": args
        }
        self.send_packet(okex_req)

    def subscribe(self, req: SubscribeRequest) -> None:
        """register market data"""
        self.subscribed[req.vt_symbol] = req

        tick: TickData = TickData(
            symbol=req.symbol,
            exchange=req.exchange,
            name=req.symbol,
            datetime=datetime.now(NY_TZ),
            gateway_name=self.gateway_name,
        )
        self.ticks[req.symbol] = tick

        args: list = []
        for channel in ["tickers", "books5"]:
            args.append({
                "channel": channel,
                "instId": req.symbol
            })

        req: dict = {
            "op": "subscribe",
            "args": args
        }
        self.send_packet(req)

    def on_connected(self) -> None:
        self.gateway.write_log("Websocket Public API Connected")

        self.query_contract()

        for req in list(self.subscribed.values()):
            self.subscribe(req)

    def on_disconnected(self) -> None:
        self.gateway.write_log("Websocket Public API Lost Connection")

    def on_packet(self, packet: dict) -> None:
        if "event" in packet:
            event: str = packet["event"]
            if event == "subscribe":
                return
            elif event == "error":
                code: str = packet["code"]
                msg: str = packet["msg"]
                self.gateway.write_log(f"Websocket Public API Exception: {code}, msg: {msg}")
        else:
            channel: str = packet["arg"]["channel"]
            callback: callable = self.callbacks.get(channel, None)

            if callback:
                data: list = packet["data"]
                callback(data)

    def on_error(self, exception_type: type, exception_value: Exception, tb) -> None:
        msg: str = f"Exception: {exception_type}, msg: {exception_value}"
        self.gateway.write_log(msg)

        sys.stderr.write(
            self.exception_detail(exception_type, exception_value, tb)
        )

    def on_instrument(self, data: list) -> None:
        for d in data:
            symbol: str = d["instId"]
            product: Product = PRODUCT_OKEX2VT[d["instType"]]
            net_position: bool = True
            if product == Product.SPOT:
                size: float = 1
            else:
                size: float = float(d["ctMult"])

            contract: ContractData = ContractData(
                symbol=symbol,
                exchange=Exchange.OKEX,
                name=symbol,
                product=product,
                size=size,
                pricetick=float(d["tickSz"]),
                min_volume=float(d["minSz"]),
                history_data=True,
                net_position=net_position,
                gateway_name=self.gateway_name,
            )

            if product == Product.OPTION:
                contract.option_strike = float(d["stk"])
                contract.option_type = OPTIONTYPE_OKEXO2VT[d["optType"]]
                contract.option_expiry = datetime.fromtimestamp(int(d["expTime"]) / 1000)
                contract.option_portfolio = d["uly"]
                contract.option_index = d["stk"]
                contract.option_underlying = "_".join([
                    contract.option_portfolio,
                    contract.option_expiry.strftime("%Y%m%d")
                ])

            symbol_contract_map[contract.symbol] = contract
            self.gateway.on_contract(contract)

        self.gateway.write_log(f"{d['instType']} succeeded")

    def on_ticker(self, data: list) -> None:
        for d in data:
            tick: TickData = self.ticks[d["instId"]]
            tick.last_price = float(d["last"])
            tick.open_price = float(d["open24h"])
            tick.high_price = float(d["high24h"])
            tick.low_price = float(d["low24h"])
            tick.volume = float(d["vol24h"])

    def on_depth(self, data: list) -> None:
        for d in data:
            tick: TickData = self.ticks[d["instId"]]
            bids: list = d["bids"]
            asks: list = d["asks"]

            for n in range(min(5, len(bids))):
                price, volume, _, _ = bids[n]
                tick.__setattr__("bid_price_%s" % (n + 1), float(price))
                tick.__setattr__("bid_volume_%s" % (n + 1), float(volume))

            for n in range(min(5, len(asks))):
                price, volume, _, _ = asks[n]
                tick.__setattr__("ask_price_%s" % (n + 1), float(price))
                tick.__setattr__("ask_volume_%s" % (n + 1), float(volume))

            tick.datetime = parse_timestamp(d["ts"])
            self.gateway.on_tick(copy(tick))


class OkexWebsocketPrivateApi(WebsocketClient):
    """"""

    def __init__(self, gateway: OkexGateway) -> None:
        super().__init__()

        self.gateway: OkexGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: str = ""
        self.passphrase: str = ""

        self.reqid: int = 0
        self.order_count: int = 0
        self.connect_time: int = 0

        self.callbacks: Dict[str, callable] = {
            "login": self.on_login,
            "orders": self.on_order,
            "account": self.on_account,
            "positions": self.on_position,
            "order": self.on_send_order,
            "cancel-order": self.on_cancel_order,
            "error": self.on_api_error
        }

        self.reqid_order_map: Dict[str, OrderData] = {}

    def connect(
        self,
        key: str,
        secret: str,
        passphrase: str,
        proxy_host: str,
        proxy_port: int,
        server: str
    ) -> None:
        self.key = key
        self.secret = secret.encode()
        self.passphrase = passphrase

        self.connect_time = int(datetime.now().strftime("%y%m%d%H%M%S"))

        if server == "REAL":
            self.init(PRIVATE_WEBSOCKET_HOST, proxy_host, proxy_port, 20)
        else:
            self.init(TEST_PRIVATE_WEBSOCKET_HOST, proxy_host, proxy_port, 20)

        self.start()

    def on_connected(self) -> None:
        self.gateway.write_log("Websocket Private API Connected")
        self.login()

    def on_disconnected(self) -> None:
        self.gateway.write_log("Websocket Private API Lost Connection")

    def on_packet(self, packet: dict) -> None:
        if "event" in packet:
            cb_name: str = packet["event"]
        elif "op" in packet:
            cb_name: str = packet["op"]
        else:
            cb_name: str = packet["arg"]["channel"]

        callback: callable = self.callbacks.get(cb_name, None)
        if callback:
            callback(packet)

    def on_error(self, exception_type: type, exception_value: Exception, tb) -> None:
        msg: str = f"ERROR: {exception_type}, msg: {exception_value}"
        self.gateway.write_log(msg)

        sys.stderr.write(
            self.exception_detail(exception_type, exception_value, tb)
        )

    def on_api_error(self, packet: dict) -> None:
        code: str = packet["code"]
        msg: str = packet["msg"]
        self.gateway.write_log(f"Websocket Private API Failed: {code}, msg: {msg}")

    def on_login(self, packet: dict) -> None:
        if packet["code"] == '0':
            self.gateway.write_log("Websocket Private API Login")
            self.subscribe_topic()
        else:
            self.gateway.write_log("Websocket Private API Failed Login")

    def on_order(self, packet: dict) -> None:
        data = packet["data"]
        for d in data:
            order: OrderData = parse_order_data(d, self.gateway_name)
            self.gateway.on_order(order)

            if d["fillSz"] == "0":
                return

            trade_volume: float = float(d["fillSz"])
            contract: ContractData = symbol_contract_map.get(order.symbol, None)
            if contract:
                trade_volume = round_to(trade_volume, contract.min_volume)

            trade: TradeData = TradeData(
                symbol=order.symbol,
                exchange=order.exchange,
                orderid=order.orderid,
                tradeid=d["tradeId"],
                direction=order.direction,
                offset=order.offset,
                price=float(d["fillPx"]),
                volume=trade_volume,
                datetime=parse_timestamp(d["uTime"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_trade(trade)

    def on_account(self, packet: dict) -> None:
        if len(packet["data"]) == 0:
            return
        buf: dict = packet["data"][0]
        for detail in buf["details"]:
            account: AccountData = AccountData(
                accountid=detail["ccy"],
                balance=float(detail["eq"]),
                gateway_name=self.gateway_name,
            )
            account.available = float(detail["availEq"])
            account.frozen = account.balance - account.available
            self.gateway.on_account(account)

    def on_position(self, packet: dict) -> None:
        data: list = packet["data"]
        for d in data:
            symbol: str = d["instId"]
            pos: int = float(d["pos"])
            price: float = get_float_value(d, "avgPx")
            pnl: float = get_float_value(d, "upl")

            position: PositionData = PositionData(
                symbol=symbol,
                exchange=Exchange.OKEX,
                direction=Direction.NET,
                volume=pos,
                price=price,
                pnl=pnl,
                gateway_name=self.gateway_name,
            )
            self.gateway.on_position(position)

    def on_send_order(self, packet: dict) -> None:
        data: list = packet["data"]

        if packet["code"] != "0":
            if not data:
                order = self.reqid_order_map[packet["id"]]
                order.status = Status.REJECTED
                self.gateway.on_order(order)
                return

        for d in data:
            code: str = d["sCode"]
            if code == "0":
                return

            orderid = d["clOrdId"]
            order = self.gateway.get_order(orderid)
            if not order:
                return
            order.status = Status.REJECTED
            self.gateway.on_order(copy(order))

            msg: str = d["sMsg"]
            self.gateway.write_log(f"Send Order Failed: {code}, msg: {msg}")

    def on_cancel_order(self, packet: dict) -> None:
        if packet["code"] != "0":
            code: str = packet["code"]
            msg: str = packet["msg"]
            self.gateway.write_log(f"Cancel Order Failed: {code}, msg: {msg}")
            return

        data: list = packet["data"]
        for d in data:
            code: str = d["sCode"]
            if code == "0":
                return

            msg: str = d["sMsg"]
            self.gateway.write_log(f"Cancel Order Failed: {code}, msg: {msg}")

    def login(self) -> None:
        timestamp: str = str(time.time())
        msg: str = timestamp + "GET" + "/users/self/verify"
        signature: bytes = generate_signature(msg, self.secret)

        okex_req: dict = {
            "op": "login",
            "args":
            [
                {
                    "apiKey": self.key,
                    "passphrase": self.passphrase,
                    "timestamp": timestamp,
                    "sign": signature.decode("utf-8")
                }
            ]
        }
        self.send_packet(okex_req)

    def subscribe_topic(self) -> None:
        okex_req = {
            "op": "subscribe",
            "args": [
                {
                    "channel": "orders",
                    "instType": "ANY"
                },
                {
                    "channel": "account"
                },
                {
                    "channel": "positions",
                    "instType": "ANY"
                },
            ]
        }
        self.send_packet(okex_req)

    def send_order(self, req: OrderRequest) -> str:
        if req.type not in ORDERTYPE_VT2OKEX:
            self.gateway.write_log(f"Type not supported: {req.type.value}")
            return

        contract: ContractData = symbol_contract_map.get(req.symbol, None)
        if not contract:
            self.gateway.write_log(f"Unable to find contract: {req.symbol}")
            return

        self.order_count += 1
        count_str = str(self.order_count).rjust(6, "0")
        orderid = f"{self.connect_time}{count_str}"

        args: dict = {
            "instId": req.symbol,
            "clOrdId": orderid,
            "side": DIRECTION_VT2OKEX[req.direction],
            "ordType": ORDERTYPE_VT2OKEX[req.type],
            "px": str(req.price),
            "sz": str(req.volume)
        }

        if contract.product == Product.SPOT:
            # args["tdMode"] = "cash"
            args["tdMode"] = "isolated"
        else:
            args["tdMode"] = "cross"

        self.reqid += 1
        okex_req: dict = {
            "id": str(self.reqid),
            "op": "order",
            "args": [args]
        }
        self.send_packet(okex_req)

        order: OrderData = req.create_order_data(orderid, self.gateway_name)
        self.gateway.on_order(order)
        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        args: dict = {"instId": req.symbol}

        if req.orderid in local_orderids:
            args["clOrdId"] = req.orderid
        else:
            args["ordId"] = req.orderid

        self.reqid += 1
        okex_req: dict = {
            "id": str(self.reqid),
            "op": "cancel-order",
            "args": [args]
        }
        self.send_packet(okex_req)


def generate_signature(msg: str, secret_key: str) -> bytes:
    return base64.b64encode(hmac.new(secret_key, msg.encode(), hashlib.sha256).digest())


def generate_timestamp() -> str:
    now: datetime = datetime.utcnow()
    timestamp: str = now.isoformat("T", "milliseconds")
    return timestamp + "Z"


def parse_timestamp(timestamp: str) -> datetime:
    dt: datetime = datetime.fromtimestamp(int(timestamp) / 1000)
    return NY_TZ.localize(dt)


def get_float_value(data: dict, key: str) -> float:
    data_str = data.get(key, "")
    if not data_str:
        return 0.0
    return float(data_str)


def parse_order_data(data: dict, gateway_name: str) -> OrderData:
    order_id: str = data["clOrdId"]
    if order_id:
        local_orderids.add(order_id)
    else:
        order_id: str = data["ordId"]

    order = OrderData(
        symbol=data["instId"],
        exchange=Exchange.OKEX,
        type=ORDERTYPE_OKEX2VT[data["ordType"]],
        orderid=order_id,
        direction=DIRECTION_OKEX2VT[data["side"]],
        offset=Offset.NONE,
        traded=float(data["accFillSz"]),
        price=float(data["px"]) if data["px"] else 0,
        volume=float(data["sz"]),
        datetime=parse_timestamp(data["cTime"]),
        status=STATUS_OKEX2VT[data["state"]],
        gateway_name=gateway_name,
    )
    return order
