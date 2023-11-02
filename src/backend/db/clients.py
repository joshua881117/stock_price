from airflow.models import Variable
from sqlalchemy import create_engine, engine
import json

def get_mysql_conn() -> engine.base.Connection:
    """連線到 mysql 資料庫"""
    mysql_config = json.loads(Variable.get('mysql_config')) # 從 airflow variable 讀取 mysql 參數
    MYSQL_DATA_USER = mysql_config['MYSQL_DATA_USER']
    MYSQL_DATA_PASSWORD = mysql_config['MYSQL_DATA_PASSWORD']
    MYSQL_DATA_HOST = mysql_config['MYSQL_DATA_HOST']
    MYSQL_DATA_PORT = mysql_config['MYSQL_DATA_PORT']
    MYSQL_DATA_DATABASE = mysql_config['MYSQL_DATA_DATABASE']
    address = (
        f"mysql+pymysql://{MYSQL_DATA_USER}:{MYSQL_DATA_PASSWORD}"
        f"@{MYSQL_DATA_HOST}:{MYSQL_DATA_PORT}/{MYSQL_DATA_DATABASE}"
    )
    engine = create_engine(address)
    connect = engine.connect()
    return connect