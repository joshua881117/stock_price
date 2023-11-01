import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
# from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.models.param import Param

import datetime as dt 
from crawler.stock_crawler import crawler_twse
from backend import db
from backend.db import db_executor
from stock_app.stock_functions import send_message_to_slack, upload_data_to_bigquery, is_data_uploaded

import os
from loguru import logger

default_args = {
    'owner': 'Joshua Lin',
    'email': ['joshua881117@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes = 5),
}

def get_data(**kwargs):
    params = kwargs['dag_run'].conf
    # 獲取 DAG 參數，如果未傳入參數則預設為今日
    logical_date = kwargs['dag_run'].logical_date.date() + dt.timedelta(days=1)
    date = params.get('date', str(logical_date))
    print(logical_date)
    df = crawler_twse(date)
    file_dir = os.path.dirname(__file__)
    file_dir = os.path.abspath(os.path.join(file_dir, os.pardir))
    file_path = os.path.join(file_dir, f'data/stock_data_{date}.csv')
    print(file_path)
    if len(df) == 0:
        return 'do_nothing'
    else:
        df.to_csv(file_path, index=False)
        return 'upload_to_db'

def upload_data(**kwargs):
    params = kwargs['dag_run'].conf
    # 獲取 DAG 參數，如果未傳入參數則預設為今日
    table = params.get('table', 'TaiwanStockPrice')
    logical_date = kwargs['dag_run'].logical_date.date() + dt.timedelta(days=1)
    date = params.get('date', str(logical_date))

    # r = db.get_db_router()
    file_dir = os.path.dirname(__file__)
    file_dir = os.path.abspath(os.path.join(file_dir, os.pardir))
    file_path = os.path.join(file_dir, f'data/stock_data_{date}.csv')
    # print(file_path)
    df = pd.read_csv(file_path)
    # with r.mysql_conn as conn:
    #     db_executor.upload_data(df, table, conn)
    #     if os.path.exists(file_path):
    #         # 刪除檔案
    #         os.remove(file_path)
    if is_data_uploaded(dataset='Joshua', table='stock_price', date=date):
        upload_data_to_bigquery(dataset='Joshua', table='stock_price', df=df, write_disposition='WRITE_APPEND')
        logger.info(f"Success upload {len(df)} data")
    else:
        logger.info("Already upload data")
    if os.path.exists(file_path):
        # 刪除檔案
        os.remove(file_path)

def send_message(**kwargs):
    params = kwargs['dag_run'].conf
    logical_date = kwargs['dag_run'].logical_date.date() + dt.timedelta(days=1)
    date = params.get('date', str(logical_date))
    send_message_to_slack(
        channel='#爬蟲通知', 
        app_name='stock_notify',
        message=f'{date} 股票資料上傳成功'
    )

with DAG(
    dag_id = 'stock_data_crawler',
    default_args = default_args,
    description = 'daily stock price crawler',
    start_date = dt.datetime(2023, 1, 1),
    schedule = '0 18 * * Mon-Fri',
    params = {
        "date": Param(str(dt.date.today()), type='string'),
        "table": Param("TaiwanStockPrice", type='string')
    }
) as dag:

    start = EmptyOperator(task_id='start')

    do_nothing = EmptyOperator(task_id='do_nothing')

    get_stock_data = BranchPythonOperator(
        task_id='get_stock_data',
        python_callable=get_data
    )

    upload_to_db = PythonOperator(
        task_id='upload_to_db',
        python_callable=upload_data
    )

    send_success_message = PythonOperator(
        task_id='send_success_message',
        python_callable=send_message
    )

    start >> get_stock_data
    get_stock_data >> [upload_to_db, do_nothing]
    upload_to_db >> send_success_message