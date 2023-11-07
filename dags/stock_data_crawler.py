import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param

import datetime as dt 
import sys
sys.path.append("/opt/airflow/src")

from crawler.stock_crawler import crawler_twse, is_weekend
from stock_app.stock_functions import send_message_to_slack, upload_data_to_bigquery, is_data_uploaded

import os
import logging

default_args = {
    'owner': 'Joshua Lin',
    'email': ['joshua881117@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes = 5),
}

def get_data(**kwargs):
    '''爬證交所股票資料'''
    params = kwargs['dag_run'].conf
    # 獲取 DAG 參數，如果未傳入參數則預設為今日
    logical_date = kwargs['dag_run'].logical_date.date()
    if logical_date.weekday() == 4:
        logical_date += dt.timedelta(days=3) # 如果是週一執行的 DAG，logical_date 為前一個執行週期的日期(週五)，因此 logical_date 需要增加 3 天
    else:
        logical_date += dt.timedelta(days=1) # 週二到週四的 logical_date 僅需要加一天
    date = params.get('date', str(logical_date))

    # 爬蟲
    df = crawler_twse(date)
    file_dir = os.path.dirname(__file__) # 目前檔案所在目錄
    file_dir = os.path.abspath(os.path.join(file_dir, os.pardir)) # 上一層目錄
    file_path = os.path.join(file_dir, f'data/stock_data_{date}.csv')
    # 如果爬蟲下來沒資料，代表今天未開盤
    if len(df) == 0:
        return 'do_nothing'
    else:
        df.to_csv(file_path, index=False)
        return 'upload_to_db'

def upload_data(**kwargs):
    '''上傳股票資料到 BigQuery'''
    params = kwargs['dag_run'].conf
    # 獲取 DAG 參數，如果未傳入參數則預設為今日
    # table = params.get('table', 'TaiwanStockPrice')

    logical_date = kwargs['dag_run'].logical_date.date()
    if logical_date.weekday() == 4:
        logical_date += dt.timedelta(days=3) # 如果是週一執行的 DAG，logical_date 為前一個執行週期的日期(週五)，因此 logical_date 需要增加 3 天
    else:
        logical_date += dt.timedelta(days=1) # 週二到週四的 logical_date 僅需要加一天

    date = params.get('date', str(logical_date))

    file_dir = os.path.dirname(__file__)
    file_dir = os.path.abspath(os.path.join(file_dir, os.pardir))
    file_path = os.path.join(file_dir, f'data/stock_data_{date}.csv')

    df = pd.read_csv(file_path)

    df['Date'] = df['Date'].apply(lambda x: dt.datetime.strptime(str(x), '%Y-%m-%d'))
    # 上傳到 mysql 資料庫
    # r = db.get_db_router()
    # with r.mysql_conn as conn:
    #     db_executor.upload_data(df, table, conn)
    #     if os.path.exists(file_path):
    #         # 刪除檔案
    #         os.remove(file_path)

    # 如果資料已經上傳就不重複上傳
    if is_data_uploaded(dataset='Joshua', table='stock_price', date=date):
        logging.info("Already upload data")
    else:
        upload_data_to_bigquery(dataset='Joshua', table='stock_price', df=df, write_disposition='WRITE_APPEND')
        logging.info(f"Success upload {len(df)} data")
    # 上傳完就刪除 csv 檔
    if os.path.exists(file_path):
        # 刪除檔案
        os.remove(file_path)

def send_message(**kwargs):
    '''發送成功上傳訊息至 slack'''
    params = kwargs['dag_run'].conf
    logical_date = kwargs['dag_run'].logical_date.date()
    if logical_date.weekday() == 4:
        logical_date += dt.timedelta(days=3) # 如果是週一執行的 DAG，logical_date 為前一個執行週期的日期(週五)，因此 logical_date 需要增加 3 天
    else:
        logical_date += dt.timedelta(days=1) # 週二到週四的 logical_date 僅需要加一天
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
        "date": Param(str(dt.date.today()), type='string')
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