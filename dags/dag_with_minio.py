from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
import datetime as dt

default_args = {
    'owner': 'Joshua Lin',
    'email': ['joshua881117@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes = 5),
}

with DAG(
    dag_id = 'dag_with_minio_v2',
    default_args = default_args,
    start_date = dt.datetime(2023, 10, 25),
    schedule = '@daily',
) as dag:
    task1 = S3KeySensor(
        task_id='sensor_minio_s3',
        bucket_name='airflow',
        bucket_key='result.csv',
        aws_conn_id='minio_conn',
        mode='poke',
        poke_interval=5,
        timeout=30
    )