# -*- coding:utf-8 -*-
"""
@FileName  :file_handler.py
@Time      :2022/12/12 13:46
@Author    :fsksf
"""
import datetime
from typing import Callable
from enum import Enum
import dbf
from vnpy.trader.constant import Status, Direction
from vnpy.trader.object import TradeData
from vnpy_qmt.utils import TO_VN_Exchange_map

from watchdog.events import FileSystemEventHandler, FileModifiedEvent


class ResultFileHandler(FileSystemEventHandler):

    def __init__(self, on_order: Callable, on_trade: Callable):
        self._on_order = on_order
        self._order_p = 0
        self._on_trade = on_trade

    def on_modified(self, event: FileModifiedEvent):
        path = event.src_path
        if path.endswith('XT_DBF_ORDER_result.dbf'):
            self.on_order_result(path)
        if path.endswith(f'XT_CJCX_Stock_{datetime.datetime.now().strftime("%Y%m%d")}.dbf'):
            self.on_trade_change(path)

    def on_trade_change(self, path):
        result_table = dbf.Table(path, codepage='cp936')
        with result_table.open(mode=dbf.READ_ONLY) as f:
            for row in f:
                if row['投资备注'].strip() == "":
                    continue
                caozuo = row['操作'].strip()
                if caozuo == '买入':
                    direction = Direction.LONG
                else:
                    direction = Direction.SHORT
                trade = TradeData(
                    symbol=row['证券代码'].strip(),
                    exchange=TO_VN_Exchange_map[row['证券市场'].strip()],
                    orderid=row['投资备注'].strip(),
                    direction=direction,
                    price=float(row['成交价格'].strip()),
                    volume=int(row['成交数量'].strip()),
                    gateway_name='QMT',
                    datetime=datetime.datetime.strptime(row['成交日期'].strip() + " " + row['成交时间'].strip(),
                                                        '%Y%m%d %H:%M:%S'),
                    tradeid=row['成交编号'].strip(),
                )
                self._on_trade(trade)

    def on_order_result(self, path):
        result_table = dbf.Table(path, codepage='cp936')
        with result_table.open(mode=dbf.READ_ONLY) as f:
            for row in f:
                try:
                    xt_order_id = str(row.ORDERNUM).strip()
                except ValueError:
                    xt_order_id = '-1'
                task_process = row.TASKPRO
                try:
                    traded, _ = task_process.split('/')
                    traded = int(traded)
                except ValueError:
                    traded = 0
                d = {
                    "xt_order_id": xt_order_id,
                    "msg": str(row.MESSAGE).strip(),
                    "status": str(row.STATUS).strip(),
                    "task_status": str(row.TASKSTATUS).strip(),
                    "note": str(row.NOTE).strip(),
                    "traded": traded
                }
                self._on_order(d)


class TaskStatus(Enum):
    # 未知
    UNKNOWN = "0"
    # 等待
    WAIT_REPORTING = "1"
    # 已报
    REPORTED = "2"
    # 已报待撤
    ORDER_REPORTED_CANCEL = "5"
    # 部成待撤
    ORDER_PARTSUCC_CANCEL = "3"
    # 部撤
    ORDER_PART_CANCEL = "8"
    # 已撤
    ORDER_CANCELED = "8"
    # 部成
    ORDER_PART_SUCC = "3"
    # 已成
    ORDER_SUCCEEDED = "7"
    # 废单
    ORDER_JUNK = "9"


TaskStatus_Status_Map = {
    TaskStatus.UNKNOWN.value: Status.SUBMITTING,
    TaskStatus.WAIT_REPORTING.value: Status.SUBMITTING,
    TaskStatus.REPORTED.value: Status.SUBMITTING,
    TaskStatus.ORDER_REPORTED_CANCEL.value: Status.SUBMITTING,
    TaskStatus.ORDER_PARTSUCC_CANCEL.value: Status.PARTTRADED,
    TaskStatus.ORDER_PART_CANCEL.value: Status.CANCELLED,
    TaskStatus.ORDER_PART_SUCC.value: Status.PARTTRADED,
    TaskStatus.ORDER_SUCCEEDED.value: Status.ALLTRADED,
    TaskStatus.ORDER_JUNK.value: Status.REJECTED
}


if __name__ == '__main__':
    ResultFileHandler(None).on_order_result(
        "D:\soft\changcheng_qmt\export_data\XT_DBF_ORDER_result.dbf")