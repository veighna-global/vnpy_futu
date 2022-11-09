import pandas as pd
from copy import copy
from datetime import datetime
from threading import Thread
from time import sleep
from typing import Any, Dict, List, Set, Tuple, Union

from futu import (
    ModifyOrderOp,
    TrdSide,
    TrdEnv,
    TrdMarket,
    KLType,
    OpenQuoteContext,
    OrderBookHandlerBase,
    OrderStatus,
    OrderType,
    RET_ERROR,
    RET_OK,
    StockQuoteHandlerBase,
    TradeDealHandlerBase,
    TradeOrderHandlerBase,
    OpenSecTradeContext,
    OpenFutureTradeContext
)

from vnpy.event import EventEngine
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Offset,
    Product,
    Status,
    Interval
)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    OrderData,
    TradeData,
    BarData,
    AccountData,
    ContractData,
    PositionData,
    SubscribeRequest,
    OrderRequest,
    CancelRequest,
    HistoryRequest
)
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.utility import ZoneInfo


# 委托状态映射
STATUS_FUTU2VT: Dict[OrderStatus, Status] = {
    OrderStatus.NONE: Status.SUBMITTING,
    OrderStatus.WAITING_SUBMIT: Status.SUBMITTING,
    OrderStatus.SUBMITTING: Status.SUBMITTING,
    OrderStatus.SUBMITTED: Status.NOTTRADED,
    OrderStatus.FILLED_PART: Status.PARTTRADED,
    OrderStatus.FILLED_ALL: Status.ALLTRADED,
    OrderStatus.CANCELLED_PART: Status.CANCELLED,
    OrderStatus.CANCELLED_ALL: Status.CANCELLED,
    OrderStatus.FAILED: Status.REJECTED,
    OrderStatus.DISABLED: Status.CANCELLED,
    OrderStatus.DELETED: Status.CANCELLED,
}

# 多空方向映射
DIRECTION_VT2FUTU: Dict[Direction, TrdSide] = {
    Direction.LONG: TrdSide.BUY,
    Direction.SHORT: TrdSide.SELL,
}
DIRECTION_FUTU2VT: Dict[TrdSide, Tuple] = {
    TrdSide.BUY: (Direction.LONG, Offset.OPEN),
    TrdSide.SELL: (Direction.SHORT, Offset.OPEN),
    TrdSide.BUY_BACK: (Direction.LONG, Offset.CLOSE),
    TrdSide.SELL_SHORT: (Direction.SHORT, Offset.CLOSE),
}

# 交易所映射
EXCHANGE_VT2FUTU: Dict[Exchange, str] = {
    Exchange.SMART: "US",
    Exchange.SEHK: "HK",
    Exchange.HKFE: "HK_FUTURE",
}
EXCHANGE_FUTU2VT: Dict[str, Exchange] = {v: k for k, v in EXCHANGE_VT2FUTU.items()}

# 产品类型映射
PRODUCT_VT2FUTU: Dict[Product, str] = {
    Product.EQUITY: "STOCK",
    Product.INDEX: "IDX",
    Product.ETF: "ETF",
    Product.WARRANT: "WARRANT",
    Product.BOND: "BOND",
    Product.FUTURES: "FUTURE"
}


# 其他常量
CHINA_TZ = ZoneInfo("Asia/Shanghai")


class FutuGateway(BaseGateway):
    """
    veighna用于对接富途证券的交易接口。
    """
    default_name: str = "FUTU"

    default_setting: Dict[str, Any] = {
        "密码": "",
        "地址": "127.0.0.1",
        "端口": 11111,
        "市场": ["HK", "US", "HK_FUTURE"],
        "环境": [TrdEnv.REAL, TrdEnv.SIMULATE],
    }

    exchanges: List[str] = list(EXCHANGE_FUTU2VT.values())

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.quote_ctx: OpenQuoteContext = None
        self.trade_ctx: Union[OpenSecTradeContext, OpenFutureTradeContext] = None

        self.host: str = ""
        self.port: int = 0
        self.market: str = ""
        self.password: str = ""
        self.env: TrdEnv = TrdEnv.SIMULATE

        self.ticks: Dict[str, TickData] = {}
        self.trades: Set = set()
        self.contracts: Dict[str, ContractData] = {}

        self.thread: Thread = Thread(target=self.query_data)

        self.count: int = 0
        self.interval: int = 3
        self.query_funcs: list = [self.query_account, self.query_position]

    def connect(self, setting: dict) -> None:
        """连接交易接口"""
        self.host: str = setting["地址"]
        self.port: int = setting["端口"]
        self.market: str = setting["市场"]
        self.password: str = setting["密码"]
        self.env: TrdEnv = setting["环境"]

        self.connect_quote()
        self.connect_trade()

        self.thread.start()

    def query_data(self) -> None:
        """查询数据"""
        sleep(2.0)  # 等待两秒直到连接成功

        self.query_contract()
        self.query_trade()
        self.query_order()
        self.query_position()
        self.query_account()

        # 初始化定时查询任务
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def process_timer_event(self, event) -> None:
        """定时事件处理"""
        self.count += 1
        if self.count < self.interval:
            return
        self.count = 0
        func = self.query_funcs.pop(0)
        func()
        self.query_funcs.append(func)

    def connect_quote(self) -> None:
        """连接行情服务端"""
        self.quote_ctx: OpenQuoteContext = OpenQuoteContext(self.host, self.port)

        class QuoteHandler(StockQuoteHandlerBase):
            gateway: FutuGateway = self

            def on_recv_rsp(self, rsp_str):
                ret_code, content = super(QuoteHandler, self).on_recv_rsp(
                    rsp_str
                )
                if ret_code != RET_OK:
                    return RET_ERROR, content
                self.gateway.process_quote(content)
                return RET_OK, content

        class OrderBookHandler(OrderBookHandlerBase):
            gateway: FutuGateway = self

            def on_recv_rsp(self, rsp_str):
                ret_code, content = super(OrderBookHandler, self).on_recv_rsp(
                    rsp_str
                )
                if ret_code != RET_OK:
                    return RET_ERROR, content
                self.gateway.process_orderbook(content)
                return RET_OK, content

        self.quote_ctx.set_handler(QuoteHandler())
        self.quote_ctx.set_handler(OrderBookHandler())
        self.quote_ctx.start()

        self.write_log("行情接口连接成功")

    def connect_trade(self) -> None:
        """连接交易服务端"""
        if self.market == "HK":
            self.trade_ctx = OpenSecTradeContext(filter_trdmarket=TrdMarket.HK, host=self.host, port=self.port,)
        elif self.market == "US":
            self.trade_ctx = OpenSecTradeContext(filter_trdmarket=TrdMarket.US, host=self.host, port=self.port,)
        elif self.market == "HK_FUTURE":
            self.trade_ctx = OpenFutureTradeContext(host=self.host, port=self.port)

        class OrderHandler(TradeOrderHandlerBase):
            gateway: FutuGateway = self

            def on_recv_rsp(self, rsp_str):
                ret_code, content = super(OrderHandler, self).on_recv_rsp(
                    rsp_str
                )
                if ret_code != RET_OK:
                    return RET_ERROR, content
                self.gateway.process_order(content)
                return RET_OK, content

        class DealHandler(TradeDealHandlerBase):
            gateway: FutuGateway = self

            def on_recv_rsp(self, rsp_str):
                ret_code, content = super(DealHandler, self).on_recv_rsp(
                    rsp_str
                )
                if ret_code != RET_OK:
                    return RET_ERROR, content
                self.gateway.process_deal(content)
                return RET_OK, content

        # 交易接口解锁
        code, data = self.trade_ctx.unlock_trade(self.password)
        if code == RET_OK:
            self.write_log("交易接口解锁成功")
        else:
            self.write_log(f"交易接口解锁失败，原因：{data}")

        # 连接交易接口
        self.trade_ctx.set_handler(OrderHandler())
        self.trade_ctx.set_handler(DealHandler())
        self.trade_ctx.start()
        self.write_log("交易接口连接成功")

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        for data_type in ["QUOTE", "ORDER_BOOK"]:
            futu_symbol: str = convert_symbol_vt2futu(req.symbol, req.exchange)
            code, data = self.quote_ctx.subscribe(futu_symbol, data_type, True)

            if code:
                self.write_log(f"订阅行情失败：{data}")

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        side: TrdSide = DIRECTION_VT2FUTU[req.direction]
        futu_order_type: OrderType = OrderType.NORMAL  # 只支持限价单

        # 设置调整价格限制
        if req.direction is Direction.LONG:
            adjust_limit: float = 0.05
        else:
            adjust_limit: float = -0.05

        futu_symbol: str = convert_symbol_vt2futu(req.symbol, req.exchange)
        code, data = self.trade_ctx.place_order(
            req.price,
            req.volume,
            futu_symbol,
            side,
            futu_order_type,
            trd_env=self.env,
            adjust_limit=adjust_limit,
        )

        if code:
            self.write_log(f"委托失败：{data}")
            return ""

        for ix, row in data.iterrows():
            orderid: str = str(row["order_id"])

        order: OrderData = req.create_order_data(orderid, self.gateway_name)
        self.on_order(order)
        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        code, data = self.trade_ctx.modify_order(
            ModifyOrderOp.CANCEL, req.orderid, 0, 0, trd_env=self.env
        )

        if code:
            self.write_log(f"撤单失败：{data}")

    def query_contract(self) -> None:
        """查询合约"""
        # get_stock_basicinfo 是没有区分future的， 区分了地区
        if self.market in ["HK", "HK_FUTURE"]:
            market = "HK"
        else:
            market = self.market

        for product, futu_product in PRODUCT_VT2FUTU.items():
            code, data = self.quote_ctx.get_stock_basicinfo(
                market, futu_product
            )

            if code:
                self.write_log(f"查询合约信息失败：{data}")
                return

            for ix, row in data.iterrows():
                symbol, exchange = convert_symbol_futu2vt(row["code"])
                contract: ContractData = ContractData(
                    symbol=symbol,
                    exchange=exchange,
                    name=row["name"],
                    product=product,
                    size=1,
                    pricetick=0.001,
                    history_data=True,
                    net_position=True,
                    gateway_name=self.gateway_name,
                )
                self.on_contract(contract)
                self.contracts[contract.vt_symbol] = contract

        self.write_log("合约信息查询成功")

    def query_account(self) -> None:
        """查询资金"""
        code, data = self.trade_ctx.accinfo_query(trd_env=self.env, acc_id=0)

        if code:
            self.write_log(f"查询账户资金失败：{data}")
            return

        for ix, row in data.iterrows():
            account: AccountData = AccountData(
                accountid=f"{self.gateway_name}_{self.market}",
                balance=float(row["total_assets"]),
                frozen=(float(row["total_assets"]) - float(row["avl_withdrawal_cash"])),
                gateway_name=self.gateway_name,
            )
            self.on_account(account)

    def query_position(self) -> None:
        """查询持仓"""
        code, data = self.trade_ctx.position_list_query(
            trd_env=self.env, acc_id=0
        )

        if code:
            self.write_log(f"查询持仓失败：{data}")
            return

        for ix, row in data.iterrows():
            symbol, exchange = convert_symbol_futu2vt(row["code"])
            pos: PositionData = PositionData(
                symbol=symbol,
                exchange=exchange,
                direction=Direction.NET,
                volume=row["qty"],
                frozen=(float(row["qty"]) - float(row["can_sell_qty"])),
                price=float(row["cost_price"]),
                pnl=float(row["pl_val"]),
                gateway_name=self.gateway_name,
            )

            self.on_position(pos)

    def query_order(self) -> None:
        """查询未成交委托"""
        code, data = self.trade_ctx.order_list_query("", trd_env=self.env)

        if code:
            self.write_log(f"查询委托失败：{data}")
            return

        self.process_order(data)
        self.write_log("委托查询成功")

    def query_trade(self) -> None:
        """查询成交"""
        code, data = self.trade_ctx.deal_list_query("", trd_env=self.env)

        if code:
            self.write_log(f"查询成交失败：{data}")
            return

        self.process_deal(data)
        self.write_log("成交查询成功")

    def close(self) -> None:
        """关闭接口"""
        if self.quote_ctx:
            self.quote_ctx.close()

        if self.trade_ctx:
            self.trade_ctx.close()

    def get_tick(self, code) -> TickData:
        """查询Tick数据"""
        tick: TickData = self.ticks.get(code, None)
        symbol, exchange = convert_symbol_futu2vt(code)
        if not tick:
            tick: TickData = TickData(
                symbol=symbol,
                exchange=exchange,
                datetime=datetime.now(CHINA_TZ),
                gateway_name=self.gateway_name,
            )
            self.ticks[code] = tick

        contract: ContractData = self.contracts.get(tick.vt_symbol, None)
        if contract:
            tick.name = contract.name

        return tick

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """查询历史数据"""
        bars: List[BarData] = []

        if req.interval != Interval.MINUTE:
            self.write_log(f"获取K线数据失败，FUTU接口暂不提供{req.interval.value}级别历史数据")
            return bars

        symbol: str = convert_symbol_vt2futu(req.symbol, req.exchange)
        start_date: str = req.start.replace(tzinfo=None).strftime("%Y-%m-%d")
        end_date: str = req.end.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")

        ret, history_df, page_req_key = self.quote_ctx.request_history_kline(code=symbol, start=start_date, end=end_date, ktype=KLType.K_1M)  # 每页5个，请求第一页
        if ret != RET_OK:
            self.write_log(f"获取K线数据失败，原因：{history_df}")
            return bars

        while page_req_key != None:  # 请求后面的所有结果
            ret, data, page_req_key = self.quote_ctx.request_history_kline(code=symbol, start=start_date, end=end_date, ktype=KLType.K_1M, page_req_key=page_req_key)   # 请求翻页后的数据
            if ret == RET_OK:
                history_df = history_df.append(data, ignore_index=True)
            else:
                self.write_log(f"{data}")

        history_df["time_key"] = pd.to_datetime(history_df["time_key"])
        history_df["time_key"] = history_df["time_key"] - pd.Timedelta(1, "m")
        history_df["time_key"] = history_df["time_key"].dt.strftime("%Y-%m-%d %H:%M:%S")

        for ix, row in history_df.iterrows():
            bar: BarData = BarData(
                gateway_name=self.gateway_name,
                symbol=req.symbol,
                exchange=req.exchange,
                datetime=generate_datetime(row["time_key"]),
                interval=Interval.MINUTE,
                volume=row["volume"],
                turnover=row["turnover"],
                open_interest=0,
                open_price=row["open"],
                high_price=row["high"],
                low_price=row["low"],
                close_price=row["close"]
            )
            bars.append(bar)

        return bars

    def process_quote(self, data) -> None:
        """报价推送"""
        for ix, row in data.iterrows():
            symbol: str = row["code"]

            date: str = row["data_date"].replace("-", "")
            time: str = row["data_time"]
            dt: datetime = datetime.strptime(f"{date} {time}", "%Y%m%d %H:%M:%S")
            dt: datetime = dt.replace(tzinfo=CHINA_TZ)

            tick: TickData = self.get_tick(symbol)
            tick.datetime = dt
            tick.open_price = row["open_price"]
            tick.high_price = row["high_price"]
            tick.low_price = row["low_price"]
            tick.pre_close = row["prev_close_price"]
            tick.last_price = row["last_price"]
            tick.volume = row["volume"]

            if "price_spread" in row:
                spread = row["price_spread"]
                tick.limit_up = tick.last_price + spread * 10
                tick.limit_down = tick.last_price - spread * 10

            self.on_tick(copy(tick))

    def process_orderbook(self, data) -> None:
        """行情信息处理推送"""
        symbol: str = data["code"]
        tick: TickData = self.get_tick(symbol)

        d: dict = tick.__dict__
        if len(data) < 5:
            return

        for i in range(5):
            bid_data = data["Bid"][i]
            ask_data = data["Ask"][i]
            n = i + 1

            d["bid_price_%s" % n] = bid_data[0]
            d["bid_volume_%s" % n] = bid_data[1]
            d["ask_price_%s" % n] = ask_data[0]
            d["ask_volume_%s" % n] = ask_data[1]

        if tick.datetime:
            self.on_tick(copy(tick))

    def process_order(self, data) -> None:
        """委托信息处理推送"""
        for ix, row in data.iterrows():
            if row["order_status"] == OrderStatus.DELETED:
                continue

            direction, offset = DIRECTION_FUTU2VT[row["trd_side"]]
            symbol, exchange = convert_symbol_futu2vt(row["code"])
            order: OrderData = OrderData(
                symbol=symbol,
                exchange=exchange,
                orderid=str(row["order_id"]),
                direction=direction,
                offset=offset,
                price=float(row["price"]),
                volume=row["qty"],
                traded=row["dealt_qty"],
                status=STATUS_FUTU2VT[row["order_status"]],
                datetime=generate_datetime(row["create_time"]),
                gateway_name=self.gateway_name,
            )

            self.on_order(order)

    def process_deal(self, data) -> None:
        """成交信息处理推送"""
        for ix, row in data.iterrows():
            tradeid: str = str(row["deal_id"])
            if tradeid in self.trades:
                continue
            self.trades.add(tradeid)

            direction, offset = DIRECTION_FUTU2VT[row["trd_side"]]
            symbol, exchange = convert_symbol_futu2vt(row["code"])
            trade: TradeData = TradeData(
                symbol=symbol,
                exchange=exchange,
                direction=direction,
                offset=offset,
                tradeid=tradeid,
                orderid=row["order_id"],
                price=float(row["price"]),
                volume=row["qty"],
                datetime=generate_datetime(row["create_time"]),
                gateway_name=self.gateway_name,
            )

            self.on_trade(trade)


def convert_symbol_futu2vt(code) -> str:
    """富途合约名称转换"""
    code_list = code.split(".")
    futu_exchange = code_list[0]
    futu_symbol = ".".join(code_list[1:])
    exchange = EXCHANGE_FUTU2VT[futu_exchange]
    return futu_symbol, exchange


def convert_symbol_vt2futu(symbol, exchange) -> str:
    """veighna合约名称转换"""
    futu_exchange: Exchange = EXCHANGE_VT2FUTU[exchange]
    return f"{futu_exchange}.{symbol}"


def generate_datetime(s: str) -> datetime:
    """生成时间戳"""
    if "." in s:
        dt: datetime = datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")
    else:
        dt: datetime = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")

    dt: datetime = dt.replace(tzinfo=CHINA_TZ)
    return dt
