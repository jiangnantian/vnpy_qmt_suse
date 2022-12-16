# -*- coding:utf-8 -*-
"""
@FileName  :td.py
@Time      :2022/11/8 17:14
@Author    :fsksf
"""
import os
import random
from typing import Dict
import dbf
import datetime
import xtquant.xttrader
from watchdog.observers import Observer
from vnpy_qmt.file_handler import ResultFileHandler, TaskStatus_Status_Map
from xtquant.xttrader import XtQuantTraderCallback, XtQuantTrader
from xtquant.xttype import (
    XtTrade, XtAsset, XtOrder, XtOrderError, XtCreditOrder, XtOrderResponse,
    XtPosition, XtCreditDeal, XtCancelError, XtCancelOrderResponse, StockAccount
)
from vnpy.trader.constant import Direction, Status, Product
from vnpy.trader.object import (
    AccountData, TradeData, OrderData, OrderRequest, PositionData
)
from vnpy_qmt.utils import (to_vn_product, to_vn_contract, to_qmt_code,
                            From_VN_Trade_Type, from_vn_price_type, TO_VN_Trade_Type,
                            timestamp_to_datetime, TO_VN_ORDER_STATUS)


class TD(XtQuantTraderCallback):

    def __init__(self, gateway, *args, **kwargs):
        super(TD, self).__init__(*args, **kwargs)
        self.gateway = gateway
        self.count = 0
        self.session_id = int(datetime.datetime.now().strftime('%H%M'))
        self.trader: XtQuantTrader = None
        self.account = None
        self.mini_path = None
        self.dbf_dir = None
        self.inited = False
        self.orders: Dict[str, OrderData] = {}
        self.traders: Dict[str, TradeData] = {}
        self.orderid_vnoid_map = {}
        self.vnoid_orderid_map = {}
        self.dbf_monitor = Observer()

    def connect(self, settings: dict):
        account = settings['交易账号']
        self.mini_path = path = settings['mini路径']
        self.dbf_dir = os.path.join(os.path.dirname(self.mini_path), 'export_data')
        session_id = random.randrange(1, 100000, 1)
        acc = StockAccount(account)
        self.account = acc
        self.trader = XtQuantTrader(path=path, session=session_id)
        self.trader.register_callback(self)
        self.trader.start()
        self.write_log('连接QMT')
        cnn_msg = self.trader.connect()
        if cnn_msg == 0:
            self.write_log('连接成功')
        else:
            self.write_log(f'连接失败：{cnn_msg}')
        sub_msg = self.trader.subscribe(account=acc)
        if sub_msg == 0:
            self.write_log(f'订阅账户成功： {sub_msg}')
            self.inited = True
        else:
            self.write_log(f'订阅账户【失败】： {sub_msg}')
        eh = ResultFileHandler(self._on_dbf_order_callback, self._on_dbf_trade_callback)
        self.dbf_monitor.schedule(event_handler=eh, path=self.dbf_dir, recursive=False)
        self.dbf_monitor.start()
        self.write_log('监控dbf文件单目录中...')

    def get_order_remark(self):
        mark = f'{str(self.session_id)}.dbf{self.count}'
        self.count += 1
        return mark

    def get_vn_orderid(self, seq):
        return f'{self.gateway.gateway_name}.{str(seq)}'

    def send_order(self, req: OrderRequest):
        if req.direction in (Direction.PURCHASE, Direction.REDEMPTION):
            return self.PURCHASE_REDEMPTION(req)
        else:
            return self.normal_order(req)

    def normal_order(self, req: OrderRequest):
        seq = self.trader.order_stock_async(
            account=self.account,
            stock_code=to_qmt_code(symbol=req.symbol, exchange=req.exchange),
            order_type=From_VN_Trade_Type[req.direction],
            price_type=from_vn_price_type(req),
            order_volume=int(req.volume),
            price=req.price,
            order_remark='',
        )
        order = OrderData(gateway_name=self.gateway.gateway_name,
                          symbol=req.symbol,
                          exchange=req.exchange,
                          orderid=str(seq),
                          type=req.type,
                          direction=req.direction,
                          offset=req.offset,
                          volume=req.volume,
                          price=req.price,
                          status=Status.SUBMITTING)
        self.orders[order.orderid] = order
        return self.get_vn_orderid(seq)

    def PURCHASE_REDEMPTION(self, req: OrderRequest):
        """
        申赎
        """
        order_type = From_VN_Trade_Type[req.direction]
        price_type = '1'
        note = self.get_order_remark()
        order_ = {
            "order_type": str(order_type),
            "price_type": str(price_type),
            "mode_price": str(req.price),
            "stock_code": req.symbol,
            "volume": str(req.volume),
            "account_id": str(self.account.account_id),
            "strategy": req.reference,
            "note": note
        }

        order = OrderData(gateway_name=self.gateway.gateway_name,
                          symbol=req.symbol,
                          exchange=req.exchange,
                          orderid=note,
                          type=req.type,
                          direction=req.direction,
                          offset=req.offset,
                          volume=req.volume,
                          price=req.price,
                          status=Status.SUBMITTING)
        self.orders[note] = order
        self.gateway.on_order(order)
        tb = dbf.Table(os.path.join(self.dbf_dir, "XT_DBF_ORDER.dbf"))
        with tb.open(mode=dbf.READ_WRITE) as f:
            f.append(order_)
        tb.close()
        return f"{self.gateway.gateway_name}.{note}"

    def cancel_order(self, order_id):
        qmt_order_id = self.vnoid_orderid_map.get(order_id)
        if qmt_order_id is None:
            return 
        return self.trader.cancel_order_stock_async(account=self.account, order_id=qmt_order_id)

    def query_account(self):
        return self.trader.query_stock_asset_async(self.account, callback=self.on_stock_asset)

    def query_position(self):
        return self.trader.query_stock_positions_async(self.account, callback=self.on_stock_positions_callback)

    def query_order(self):
        self.trader.query_stock_orders_async(self.account, callback=self.on_stock_order_callback)

    def query_trade(self):
        self.trader.query_stock_trades_async(self.account, callback=self.on_stock_trade_callback)

    def on_disconnected(self):
        pass

    def on_stock_asset(self, asset: XtAsset):
        account = AccountData(
            accountid=asset.account_id,
            frozen=asset.frozen_cash,
            balance=asset.total_asset,
            position=asset.market_value,
            gateway_name=self.gateway.gateway_name
        )
        self.gateway.on_account(account)

    def on_stock_order_callback(self, order_list):
        for order in order_list:
            self.on_stock_order(order)

    def on_stock_positions_callback(self, pos_list):
        for pos in pos_list:
            self.on_stock_position(pos)

    def on_stock_trade_callback(self, trade_list):
        for trade in trade_list:
            self.on_stock_trade(trade)

    def on_stock_order(self, order: XtOrder):
        symbol, exchange = to_vn_contract(order.stock_code)
        vnpy_oid = self.orderid_vnoid_map.get(order.order_id)
        order_ = OrderData(
            orderid=vnpy_oid,
            symbol=symbol,
            exchange=exchange,
            price=order.price,
            volume=order.order_volume,
            traded=order.traded_volume,
            gateway_name=self.gateway.gateway_name,
            status = TO_VN_ORDER_STATUS[order.order_status],
            direction=TO_VN_Trade_Type[order.order_type],
            datetime=timestamp_to_datetime(order.order_time)

        )
        old_order = self.orders.get(order_.orderid, None)
        if old_order == order_:
            return
        if order_.status == Status.REJECTED:
            self.write_log(f'【拒单】 {order.status_msg}')
        self.orders[order_.orderid] = order_
        self.gateway.on_order(order_)

    def _on_dbf_trade_callback(self, trade: TradeData):
        order = self.orders.get(trade.orderid)
        if not order:
            return
        trade.gateway_name = self.gateway.gateway_name
        trade.__post_init__()
        if order.direction == Direction.PURCHASE:
            if trade.vt_symbol != order.vt_symbol:
                trade.direction = Direction.SHORT
        if order.direction == Direction.REDEMPTION:
            if trade.vt_symbol != order.vt_symbol:
                trade.direction = Direction.LONG
        self.gateway.on_trade(trade)

    def _on_dbf_order_callback(self, row: dict):
        note = row['note']
        task_status = row['task_status']
        old_order = self.orders.get(note)
        if old_order is None:
            return
        self.orderid_vnoid_map[row['xt_order_id']] = note
        self.vnoid_orderid_map[note] = row['xt_order_id']
        vn_status = TaskStatus_Status_Map.get(task_status)
        old_order.status = vn_status
        old_order.traded = row['traded']
        self.gateway.on_order(old_order)

    def on_stock_position(self, position: XtPosition):
        symbol, exchange = to_vn_contract(position.stock_code)
        # TODO ETF相关字段处理
        position_ = PositionData(
            gateway_name=self.gateway.gateway_name,
            symbol=symbol,
            exchange=exchange,
            direction=Direction.LONG,
            volume=position.volume,
            yd_volume=position.yesterday_volume,
            price=position.open_price,
            sell_able=position.can_use_volume,
            pnl=position.market_value - position.volume * position.open_price
        )
        contract = self.gateway.get_contract(position_.vt_symbol)
        position_.product = contract.product
        position_.__post_init__()
        self.gateway.on_position(position_)

    def on_stock_trade(self, trade: XtTrade):
        symbol, exchange = to_vn_contract(trade.stock_code)
        vnoid = self.orderid_vnoid_map.get(trade.order_id)
        order = self.orders.get(trade.order_id)
        trd_typ = TO_VN_Trade_Type[trade.order_type]
        trade_ = TradeData(
            gateway_name=self.gateway.gateway_name,
            symbol=symbol,
            exchange=exchange,
            orderid=vnoid,
            tradeid=trade.traded_id,
            price=trade.traded_price,
            datetime=timestamp_to_datetime(trade.traded_time),
            volume=trade.traded_volume,
            direction=trd_typ
        )

        if order.direction in (Direction.PURCHASE, Direction.REDEMPTION):
            contract = self.gateway.get_contract(trade_.vt_symbol)
            if not contract:
                if contract.product != Product.ETF:
                    if trade_.direction == Direction.LONG:
                        trade_.direction = Direction.SHORT
                    else:
                        trade_.direction = Direction.LONG
                    trade_.__post_init__()
        self.gateway.on_trade(trade_)

    def on_cancel_error(self, cancel_error: XtCancelError):
        self.write_log(cancel_error.error_msg)

    def on_order_error(self, order_error: XtOrderError):
        self.write_log(f'订单错误：{order_error.error_msg}')
        vnpy_oid = self.orderid_vnoid_map.get(order_error.order_id)
        old_order = self.orders.get(vnpy_oid, None)
        if old_order:
            old_order.status = Status.REJECTED
            self.gateway.on_order(old_order)

    def on_order_stock_async_response(self, response: XtOrderResponse):
        self.write_log(f'下单成功 {response.order_id} {response.order_remark} {response.strategy_name}')

        self.orderid_vnoid_map[response.order_id] = str(response.seq)
        self.vnoid_orderid_map[str(response.seq)] = response.order_id

    def on_cancel_order_stock_async_response(self, response: XtCancelOrderResponse):
        self.write_log(f'撤单结果： {response.cancel_result}')

    def write_log(self, msg):
        self.gateway.write_log(f'[ td ] {msg}')
