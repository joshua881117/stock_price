import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.utils.weekday import WeekDay
import datetime as dt 
from crawler.stock_crawler import is_weekend, crawler_twse
from stock_app.stock_functions import (
    generate_message, check_target_stock_price, 
    upload_file_to_slack, read_target_stock_sheet,
    send_message_to_slack
)
import os

default_args = {
    'owner': 'Joshua Lin',
    'start_date': dt.datetime(2023, 10, 22),
    'schedule_interval': '@daily',
    'email': ['joshua881117@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes = 5)
}

def is_holiday(date):
    weekday = date.weekday()
    if is_weekend(weekday):
        return 'market_closed'
    else:
        return 'get_stock_price'

def get_stock_data(date):
    date = str(date)
    df = crawler_twse(date)
    file_dir = os.path.dirname(__file__)
    file_path = os.path.join(file_dir, 'data/stock_data.csv')
    df.to_csv(file_path, index=False)
    if len(df) == 0:
        return 'market_closed'
    else:
        return 'get_target'

def get_target_price():
    sheet_id = '1emVQoQWeMqpAjfW155i3mWLQ2Cqo0OeQFhH4ZNlRR_w'
    target_df = read_target_stock_sheet(sheet_id, '目標股票')
    return target_df

def check_price(**context):
    target_df = context['task_instance'].xcom_pull(task_ids='get_target')
    file_dir = os.path.dirname(__file__)
    file_path = os.path.join(file_dir, 'data/stock_data.csv')
    df = pd.read_csv(file_path)
    ID_list, result = check_target_stock_price(df, target_df)
    result_path = os.path.join(file_dir, 'data/result.csv')
    result.to_csv(result_path, index=False)
    return ID_list

def is_meet_target(**context):
    ID_list = context['task_instance'].xcom_pull(task_ids='check_stock_price')
    if len(ID_list) == 0:
        return 'do_nothing'
    else:
        return 'send_stock_message'

def send_message_and_file(**context):
    ID_list = context['task_instance'].xcom_pull(task_ids='check_stock_price')
    message = generate_message(ID_list)

    file_dir = os.path.dirname(__file__)
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
    schedule_interval='0 18 * * *') as dag:

    make_weekend_choice = BranchDayOfWeekOperator(
        task_id="make_weekend_choice",
        follow_task_ids_if_true="market_closed",
        follow_task_ids_if_false="get_stock_price",
        week_day={WeekDay.SATURDAY, WeekDay.SUNDAY}
    )
    
    get_stock_price = BranchPythonOperator(
        task_id='get_stock_price',
        python_callable=get_stock_data,
        op_kwargs={'date':dt.date.today()}
    )
    get_target = PythonOperator(
        task_id='get_target',
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

    make_weekend_choice >> [market_closed, get_stock_price]
    get_stock_price >> [market_closed, get_target]
    get_target >> check_stock_price >> stock_meet_target
    stock_meet_target >> [do_nothing, send_stock_message]