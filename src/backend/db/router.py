import time
import typing

import logging
from sqlalchemy import engine, text
from backend.db import clients

def check_alive(
    connect: engine.base.Connection,
):
    """確認連線是否能執行query"""
    trans = connect.begin()
    sql_query = text('SELECT 1+1')
    connect.execute(sql_query)
    trans.commit()

def check_connect_alive(
    connect: engine.base.Connection,
    connect_func: typing.Callable,
    connect_count: int
):
    """確認連線是否存在"""
    if connect:
        try:
            check_alive(connect)
            logging.info("success reconnect")
            return connect
        except Exception as e:
            logging.info(f"""{connect_func.__name__} reconnect, error: {e}""")
            time.sleep(1)
            try:
                connect = connect_func() # 連線失敗重新嘗試
            except Exception as e:
                logging.info(f"""{connect_func.__name__} connect error, error: {e}""")
            connect_count += 1 
            # 重新連線次數超過 5 次就停止
            if connect_count < 5: 
                return check_connect_alive(connect, connect_func, connect_count)
            else:
                logging.info("reconnect too many times")

class Router:
    def __init__(self):
        self._mysql_conn = (
            clients.get_mysql_conn()
        )

    def check_mysql_conn_alive(self):
        self._mysql_conn = check_connect_alive(
            self._mysql_conn,
            clients.get_mysql_conn,
            0
        )
        return self._mysql_conn

    # 定義 mysql_conn 為 property 但實際上是用 mysql_conn() 來獲取值
    @property
    def mysql_conn(self):
        return (
            self.check_mysql_conn_alive()
        )
