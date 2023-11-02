import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.utils.weekday import WeekDay
from airflow.models.param import Param

import datetime as dt 

import sys
sys.path.append("/opt/airflow/src")
from crawler.stock_crawler import is_weekend
from stock_app.stock_functions import (
    generate_stock_message, check_target_stock_price, 
    upload_file_to_slack, read_target_stock_sheet,
    send_message_to_slack, query_data_from_bigquery
)
import os

default_args = {
    'owner': 'Joshua Lin',
    'start_date': dt.datetime(2023, 10, 22),
    'email': ['joshua881117@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes = 5)
}

def is_holiday(**kwargs):
    params = kwargs['dag_run'].conf
    # 獲取 DAG 參數，如果未傳入參數則預設為今日
    logical_date = kwargs['dag_run'].logical_date.date() + dt.timedelta(days=1)
    date = params.get('date', str(logical_date))
    date = dt.datetime.strptime(date, '%Y-%m-%d')

    weekday = date.weekday()
    if is_weekend(weekday):
        return 'market_closed'
    else:
        return 'get_buy_record'

def get_buy_price():
    sheet_id = '1emVQoQWeMqpAjfW155i3mWLQ2Cqo0OeQFhH4ZNlRR_w'
    buy_df = read_target_stock_sheet(sheet_id, '股票庫存')
    buy_df.rename(
        columns={
            '股票代碼': 'stockID',
            '買入價': 'buyPrice',
            '買入股數': 'buyVolume'
        }, 
        inplace=True
    )

    buy_df['buyPrice'] = pd.to_numeric(buy_df['buyPrice'])
    buy_df['buyVolume'] = pd.to_numeric(buy_df['buyVolume'])
    return buy_df

def calculate_avg_price(ti):
    buy_df = ti.xcom_pull(task_ids='get_buy_record')
    buy_df['totalValue'] = buy_df.buyPrice * buy_df.buyVolume
    buy_df = buy_df.groupby(by=['stockID']).sum().reset_index()
    buy_df['avgPrice'] = buy_df.totalValue / buy_df.buyVolume
    return buy_df[['stockID', 'avgPrice']]

def get_target_price():
    sheet_id = '1emVQoQWeMqpAjfW155i3mWLQ2Cqo0OeQFhH4ZNlRR_w'
    target_df = read_target_stock_sheet(sheet_id, '目標')
    target_df.rename(
        columns={
            '股票代碼': 'stockID',
            '最高漲幅': 'upPct',
            '最低跌幅': 'downPct'
        }, 
        inplace=True
    )

    target_df['upPct'] = pd.to_numeric(target_df['upPct'])
    target_df['downPct'] = pd.to_numeric(target_df['downPct'])
    return target_df

def get_stock_data(ti, **kwargs):
    params = kwargs['dag_run'].conf
    # 獲取 DAG 參數，如果未傳入參數則預設為今日
    logical_date = kwargs['dag_run'].logical_date.date() + dt.timedelta(days=1)
    date = params.get('date', str(logical_date))

    buy_df = ti.xcom_pull(task_ids='get_avg_price')
    stockID = list(buy_df['stockID'])
    stockID_str = str(stockID).strip('[]')

    sql_query = f"""
        SELECT StockID, Close
        FROM Joshua.stock_price
        WHERE Date = '{date}'
            AND stockID in ({stockID_str})
    """
    stock_df = query_data_from_bigquery(sql_query)
    return stock_df

def is_market_opened(ti):
    stock_df = ti.xcom_pull(task_ids='get_stock_record')
    if len(stock_df) == 0:
        return 'market_closed'
    else:
        return 'check_stock_price'

def check_price(ti):
    buy_df = ti.xcom_pull(task_ids='get_avg_price')
    target_df = ti.xcom_pull(task_ids='get_target_record')
    stock_df = ti.xcom_pull(task_ids='get_stock_record')

    file_dir = os.path.dirname(__file__)
    file_dir = os.path.abspath(os.path.join(file_dir, os.pardir)) # 上一層的路揍

    up_list, down_list, result = check_target_stock_price(stock_df, buy_df, target_df)
    result_path = os.path.join(file_dir, 'data/result.csv')
    result.to_csv(result_path, index=False)
    return up_list, down_list

def is_meet_target(ti):
    up_list, down_list = ti.xcom_pull(task_ids='check_stock_price')
    if len(up_list) + len(down_list) == 0:
        return 'do_nothing'
    else:
        return 'send_stock_message'

def send_message_and_file(ti):
    up_list, down_list = ti.xcom_pull(task_ids='check_stock_price')
    message = generate_stock_message(up_list, down_list)

    file_dir = os.path.dirname(__file__)
    file_dir = os.path.abspath(os.path.join(file_dir, os.pardir))
    result_path = os.path.join(file_dir, 'data/result.csv')
    result_file = pd.read_csv(result_path)
    upload_file_to_slack(
        channel='#股票到價通知', 
        app_name='stock_notify',
        message=message,
        file=result_file,
        file_name='result'
    )

def send_market_closed_message():
    send_message_to_slack(
        channel='#股票到價通知',
        app_name='stock_notify',
        message='今日休市'
    )

with DAG(dag_id = 'stock_notification',
    default_args = default_args,
    description = 'daily stock price notify',
    schedule_interval='0 18 * * Mon-Fri',
    params = {
        "date": Param(str(dt.date.today()), type='string')
    }
) as dag:

    check_is_weekend = BranchPythonOperator(
        task_id="check_is_weekend",
        python_callable=is_holiday
    )
    
    get_buy_record = PythonOperator(
        task_id='get_buy_record',
        python_callable=get_buy_price
    )

    get_avg_price = PythonOperator(
        task_id='get_avg_price',
        python_callable=calculate_avg_price
    )

    get_stock_record = PythonOperator(
        task_id='get_stock_record',
        python_callable=get_stock_data
    )

    check_market_opened = BranchPythonOperator(
        task_id='check_market_opened',
        python_callable=is_market_opened
    )

    get_target_record = PythonOperator(
        task_id='get_target_record',
        python_callable=get_target_price
    )

    check_stock_price = PythonOperator(
        task_id='check_stock_price',
        python_callable=check_price
    )

    stock_meet_target = BranchPythonOperator(
        task_id='stock_meet_target',
        python_callable=is_meet_target
    )
    
    send_stock_message = PythonOperator(
        task_id="send_stock_message",
        python_callable=send_message_and_file
    )

    market_closed = PythonOperator(
        task_id='market_closed',
        python_callable=send_market_closed_message
    )

    do_nothing = EmptyOperator(task_id = 'do_nothing')

    check_is_weekend >> [market_closed, get_buy_record]
    get_buy_record >> get_avg_price >> [get_stock_record, get_target_record]
    get_stock_record >> check_market_opened >> [check_stock_price, market_closed]
    get_target_record >> check_stock_price
    check_stock_price >> stock_meet_target >> [do_nothing, send_stock_message]