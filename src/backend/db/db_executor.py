import typing
import pandas as pd
import pymysql
import logging
from sqlalchemy import engine, text

def update_by_pandas(
    df: pd.DataFrame,
    table: str,
    mysql_conn: engine.base.Connection,
):
    """用 pandas 內建 function 更新資料到 mysql 資料庫"""
    if len(df) > 0:
        try:
            df.to_sql(
                name=table,
                con=mysql_conn,
                if_exists="append",
                index=False,
                chunksize=1000,
            )
        except Exception as e:
            logging.info(e)
            return False
    return True


def build_update_sql(
    colname: typing.List[str],
    value: typing.List[str],
) -> str:
    """創建更新資料的部分 sql query"""
    update_sql = ",".join([
            ' `{}` = "{}" '.format(colname[i], str(value[i]),)
            for i in range(len(colname)) if str(value[i])
        ]
    )
    return update_sql


def build_df_update_sql(
    table: str, df: pd.DataFrame
) -> typing.List[str]:
    """創建更新資料至 mysql 的 sql qyery"""
    logging.info("build_df_update_sql")
    df_columns = list(df.columns) 
    sql_list = []
    for i in range(len(df)):
        temp = list(df.iloc[i])
        value = [pymysql.converters.escape_string(str(v)) for v in temp] # 確保特殊字符（如引號）不會干擾 SQL 查詢的正確執行
        sub_df_columns = [df_columns[j] for j in range(len(temp))]
        update_sql = build_update_sql(sub_df_columns, value)
        # SQL 上傳資料方式
        # DUPLICATE KEY UPDATE 意思是
        # 如果有重複，就改用 update 的方式
        # 避免重複上傳
        sql = """INSERT INTO `{}`({})VALUES ({}) ON DUPLICATE KEY UPDATE {}
            """.format(
            table,
            "`{}`".format("`,`".join(sub_df_columns)),
            '"{}"'.format('","'.join(value)),
            update_sql,
        )
        sql_list.append(sql) # 將每一筆 sql query 存入 list
    return sql_list

def commit(
    sql: typing.Union[str, typing.List[str]],
    mysql_conn: engine.base.Connection = None,
):
    """一筆一筆更新資料到 mysql"""
    logging.info("commit")
    try:
        trans = mysql_conn.begin()
        if isinstance(sql, list): # 如果 sql 為 list 則用迴圈一筆一筆更新資料
            for s in sql:
                try:
                    sql_query = text(s)
                    mysql_conn.execution_options(autocommit=False).execute(sql_query)
                    trans.commit()
                except Exception as e:
                    logging.info(e)
                    return False

        elif isinstance(sql, str):
            try:
                sql_query = text(sql)
                mysql_conn.execution_options(autocommit=False).execute(sql_query)
                trans.commit()
            except Exception as e:
                return False
    except Exception as e:
        trans.rollback() # 出錯則回覆到資料庫原本狀態
        logging.info(e)
        return False
    return True

def update_by_sql(
    df: pd.DataFrame,
    table: str,
    mysql_conn: engine.base.Connection,
):
    """更新資料到 mysql 資料庫"""
    sql = build_df_update_sql(table, df)
    
    return commit(sql=sql, mysql_conn=mysql_conn)

def upload_data(
    df: pd.DataFrame,
    table: str,
    mysql_conn: engine.base.Connection,
):
    """上傳資料到 mysql 資料庫(pandas 和用資料庫更新兩種方法合併)"""
    if len(df) > 0:
        # 直接上傳
        if update_by_pandas(df=df, table=table, mysql_conn=mysql_conn):
            logging.info(f"Success upload {len(df)} data by pandas")
            pass
        elif update_by_sql(df=df, table=table, mysql_conn=mysql_conn):
            # 如果有重複的資料
            # 使用 SQL 語法上傳資料
            logging.info(f"Success upload {len(df)} data by SQL")
        else:
            logging.info("Upload failed")

def search_data(
    condition: str, 
    target_columns: str,
    table: str, 
    mysql_conn: engine.base.Connection
) -> typing.List:
    """從 mysql 資料庫中撈取資料"""
    try:
        sql_query = text(f"SELECT {target_columns} FROM {table} WHERE {condition}")
        result = mysql_conn.execute(sql_query).all()
        return result
    except Exception as e:
        logging.info(e)
