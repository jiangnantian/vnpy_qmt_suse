# -*- coding:utf-8 -*-
"""
@FileName  :qmt_gateway.py
@Time      :2022/11/8 16:49
@Author    :fsksf
"""
from collections import defaultdict
from typing import Dict, List
from vnpy.event import Event, EventEngine
from vnpy.trader.event import (
    EVENT_TIMER, EVENT_SNAPSHOT, EVENT_BASKET_COMPONENT,
    EVENT_TICK, EVENT_UNIMPORTANT_TICK
)
from vnpy.trader.constant import (
    Product, Direction, OrderType, Exchange

)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    ContractData,
    BasketComponent
)

from vnpy_qmt.md import MD
from vnpy_qmt.td import TD


class QmtGateway(BaseGateway):

    default_setting: Dict[str, str] = {
        "交易账号": "",
        "mini路径": ""
    }

    TRADE_TYPE = (Product.ETF, Product.EQUITY, Product.BOND, Product.INDEX)
    exchanges = (Exchange.SSE, Exchange.SZSE)

    def __init__(self, event_engine: EventEngine, gateway_name: str = 'QMT'):
        super(QmtGateway, self).__init__(event_engine, gateway_name)
        self.contracts: Dict[str, ContractData] = {}
        self.md = MD(self)
        self.td = TD(self)
        self.components: Dict[str, List[BasketComponent]] = defaultdict(list)
        self.count = -1
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)


    def connect(self, setting: dict) -> None:
        self.md.connect(setting)
        self.td.connect(setting)

    def close(self) -> None:
        pass

    def subscribe(self, req: SubscribeRequest) -> None:
        return self.md.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        return self.td.send_order(req)

    def send_basket_order(self, req: OrderRequest):

        order_list = []
        if req.direction == Direction.BUY_BASKET:
            direction = Direction.LONG
        else:
            direction = Direction.SHORT
        comp_list = self.components.get(req.vt_symbol)
        if comp_list is None:
            self.write_log(f'找不到 {req.vt_symbol} 对应的篮子')
            return (None, None)
        for comp in comp_list:
            vol = int(comp.share * req.volume)
            if vol <= 0:
                # 份额有等于0的情况
                continue
            if comp.exchange != req.exchange:
                # 篮子只下与ETF同市场的
                continue
            rq = OrderRequest(
                symbol=comp.symbol,
                direction=direction,
                type=OrderType.BestOrLimit,
                volume=vol,
                offset=req.offset,
                reference=req.reference,
                exchange=comp.exchange
            )
            order_list.append(self.td.send_order)
        return order_list

    def cancel_order(self, req: CancelRequest) -> None:
        return self.td.cancel_order(req.orderid)

    def query_account(self) -> None:
        self.td.query_account()

    def query_position(self) -> None:
        self.td.query_position()

    def query_order(self):
        self.td.query_order()

    def query_trade(self):
        self.td.query_trade()

    def on_contract(self, contract):
        self.contracts[contract.vt_symbol] = contract
        super(QmtGateway, self).on_contract(contract)

    def on_basket_component(self, comp: BasketComponent):
        self.components[comp.basket_name].append(comp)
        evt = Event(EVENT_BASKET_COMPONENT, comp)
        self.event_engine.put(evt)

    def get_contract(self, vt_symbol):
        return self.contracts.get(vt_symbol)

    def process_timer_event(self, event) -> None:
        if not self.td.inited:
            return
        if self.count == -1:
            self.query_trade()
        self.count += 1
        self.query_trade()
        if self.count < 21:
            return
        self.query_account()
        self.query_position()
        self.query_order()
        self.count = 0

    def write_log(self, msg):
        super(QmtGateway, self).write_log(f"[QMT] {msg}")


if __name__ == '__main__':
    qmt = QmtGateway(None)
    qmt.subscribe(SubscribeRequest(symbol='000001', exchange=Exchange.SZSE))
    qmt.md.get_contract()

    import threading
    import time

    def slp():
        while True:
            time.sleep(0.1)
    t = threading.Thread(target=slp)
    t.start()
    t.join()