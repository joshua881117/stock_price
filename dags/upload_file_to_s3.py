from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import datetime as dt
from tempfile import NamedTemporaryFile
from loguru import logger
import logging

from crawler.stock_crawler import crawler_twse

default_args = {
    'owner': 'Joshua Lin',
    'email': ['joshua881117@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes = 5),
}

def get_file_and_upload(**kwargs):
    logical_date = kwargs['dag_run'].logical_date.date() + dt.timedelta(days=1)
    df = crawler_twse(str(logical_date))
    with NamedTemporaryFile(mode="w", suffix=f"{logical_date}.csv") as f:
        df.to_csv(f.name, index=False)
        s3hook = S3Hook(aws_conn_id='minio_conn')
        s3hook.load_file(
            filename=f.name,
            key=f'stock_data/{logical_date}.csv',
            bucket_name='airflow',
            replace=True
        )
        logging.info(f"Success upload {f.name} file to S3")


with DAG(
    dag_id = 'upload_file_to_s3',
    default_args = default_args,
    start_date = dt.datetime(2023, 10, 25),
    schedule = '@daily'
) as dag:
    task1 = PythonOperator(
        task_id='get_and_upload_file',
        python_callable=get_file_and_upload
    )