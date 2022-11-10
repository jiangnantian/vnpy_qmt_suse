# -*- coding:utf-8 -*-
"""
@FileName  :md.py
@Time      :2022/11/8 17:14
@Author    :fsksf
"""
import datetime

from vnpy.trader.gateway import BaseGateway
from vnpy.trader.constant import (
    Exchange, Product
)
from vnpy.trader.object import (
    CancelRequest, OrderRequest, SubscribeRequest, TickData,
    ContractData
)
import xtquant.xtdata
import xtquant.xttrader
import xtquant.xttype
from vnpy_qmt.utils import (
    From_VN_Exchange_map, TO_VN_Exchange_map, to_vn_contract,
    TO_VN_Product, to_vn_product, timestamp_to_datetime
)


class MD:

    def __init__(self, gateway):
        self.gateway = gateway

    def close(self) -> None:
        pass

    def subscribe(self, req: SubscribeRequest) -> None:
        return xtquant.xtdata.subscribe_quote(
            stock_code=f'{req.symbol}.{From_VN_Exchange_map[req.exchange]}',
            period='tick',
            callback=self.on_tick
        )

    def connect(self, setting: dict) -> None:

        self.get_contract()

    def get_contract(self):
        self.write_log('开始获取标的信息')
        for sector in xtquant.xtdata.get_sector_list():
            stock_list = xtquant.xtdata.get_stock_list_in_sector(sector_name=sector)
            for symbol in stock_list:
                info = xtquant.xtdata.get_instrument_detail(symbol)
                contract_type = xtquant.xtdata.get_instrument_type(symbol)
                if info is None or contract_type is None:
                    continue
                try:
                    exchange = TO_VN_Exchange_map[info['ExchangeID']]
                except KeyError:
                    print('本gateway不支持的标的', symbol)
                if exchange not in self.gateway.exchanges:
                    continue
                product = to_vn_product(contract_type)
                if product not in self.gateway.TRADE_TYPE:
                    continue
                c = ContractData(
                    gateway_name=self.gateway.gateway_name,
                    symbol=info['InstrumentID'],
                    exchange=exchange,
                    name=info['InstrumentName'],
                    product=product,
                    pricetick=info['PriceTick'],
                    size=100,
                    min_volume=100
                )
                self.gateway.on_contract(c)
        self.write_log('获取标的信息完成')

    def on_tick(self, datas):
        for code, data_list in datas.items():
            symbol, suffix = code.rsplit('.')
            exchange = TO_VN_Exchange_map[suffix]
            for data in data_list:
                ask_price = data['askPrice']
                ask_vol = data['askVol']
                bid_price = data['bidPrice']
                bid_vol = data['bidVol']
                tick = TickData(
                    gateway_name=self.gateway.gateway_name,
                    symbol=symbol,
                    exchange=exchange,
                    datetime=timestamp_to_datetime(data['time']),
                    last_price=data['lastPrice'],
                    volume=data['volume'],
                    open_price=data['open'],
                    high_price=data['high'],
                    low_price=data['low'],
                    pre_close=data['lastClose'],
                    limit_down=0,
                    limit_up=0,
                    ask_price_1=ask_price[0],
                    ask_price_2=ask_price[1],
                    ask_price_3=ask_price[2],
                    ask_price_4=ask_price[3],
                    ask_price_5=ask_price[4],

                    ask_volume_1=ask_vol[0],
                    ask_volume_2=ask_vol[1],
                    ask_volume_3=ask_vol[2],
                    ask_volume_4=ask_vol[3],
                    ask_volume_5=ask_vol[4],

                    bid_price_1=bid_price[0],
                    bid_price_2=bid_price[1],
                    bid_price_3=bid_price[2],
                    bid_price_4=bid_price[3],
                    bid_price_5=bid_price[4],

                    bid_volume_1=bid_vol[0],
                    bid_volume_2=bid_vol[1],
                    bid_volume_3=bid_vol[2],
                    bid_volume_4=bid_vol[3],
                    bid_volume_5=bid_vol[4],
                )
                self.gateway.on_tick(tick)
    def write_log(self, msg):
        self.gateway.write_log(f"[ md ] {msg}")