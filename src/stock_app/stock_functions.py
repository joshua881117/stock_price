import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import gspread
from google.cloud import bigquery
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials
from airflow.models import Variable
import os
import json
def generate_stock_message(up_list, down_list):
    '''產生要傳送到 slack 的訊息'''
    n1 = len(up_list)
    n2 = len(down_list)
    if n1 > 0 and n2 > 0:
        message = f"今日有{n1}支股票達到目標漲幅，股票代碼為 " + ", ".join(up_list) \
            + "\n" + f"並有{n2}支股票達到目標跌幅，股票代碼為 " + ", ".join(down_list)
    elif n1 > 0:
        message = f"今日有{n1}支股票達到目標漲幅，股票代碼為 " + ", ".join(up_list)
    elif n2 > 0:
        message = f"今日有{n2}支股票達到目標跌幅，股票代碼為 " + ", ".join(up_list)
    return message

def check_target_stock_price(stock_df, buy_df, target_df):
    '''確認今日價格是否有超越目標價格'''
    up_list = []
    down_list = []
    result = pd.DataFrame()

    target_df = target_df.merge(buy_df, on='stockID', how='inner')

    for index, row in target_df.iterrows():
        stockID = row['stockID']
        up_percent = row['upPct']
        down_percent = -row['downPct']
        stock_price = stock_df[stock_df['StockID'] == stockID]['Close'].values[0]
        buy_price = row['avgPrice']

        change_percent = (stock_price - buy_price)/100

        if change_percent > up_percent:
            up_list.append(stockID)
            status = '賣出獲利'
        elif change_percent < down_percent:
            down_list.append(stockID)
            status = '賣出停損'
        else:
            status = '繼續持有'
        data = {
            'stockID': stockID, 
            'change(%)': f"{round(change_percent*100, 2)}%", 
            'avg_buy_price': round(buy_price, 2),
            'close': stock_price,
            'status': status
        }
        r = pd.DataFrame(data=data, index=[0])
        result = pd.concat([result, r])
    return up_list, down_list, result

def connect_to_slack(channel, app_name):
    '''建立 slack app 連線'''

    oauth_token = Variable.get(app_name)
    client = WebClient(token=oauth_token)
    channel_ids = {
        '#測試用':'C05UV1R79M4',
        '#股票到價通知':'C0620569DU6',
        '#爬蟲通知':'C0633KT9Z1P'
    }
    channel_id = channel_ids[channel]
    return client, channel_id
def upload_file_to_slack(channel, app_name, message, file, file_name):
    '''上傳檔案至 slack channel'''
    client, channel_id = connect_to_slack(channel, app_name)
    try:
        result = client.files_upload(
            channels=channel_id,
            initial_comment=message,
            filename=file_name,
            content=file,
        )
    except SlackApiError as e:
        print("Error uploading file: {}".format(e))

def send_message_to_slack(channel, app_name, message):
    '''上傳檔案至 slack channel'''
    client, channel_id = connect_to_slack(channel, app_name)
    try:
        result = client.chat_postMessage(
            channel=channel_id,
            text=message,
        )
    except SlackApiError as e:
        print("Error sending message: {}".format(e))

def connect_to_gsheet():
    '''建立 google sheet API 連線'''
    scopes = ["https://spreadsheets.google.com/feeds"] 
    creds = json.loads(Variable.get('gsheet_key'))
    credentials = service_account.Credentials.from_service_account_info(creds, scopes=scopes)
    client = gspread.authorize(credentials)
    return client

def read_target_stock_sheet(sheet_id, ws_name):
    '''讀取目標股價 google sheet'''
    client = connect_to_gsheet()
    sheet = client.open_by_key(sheet_id)
    ws = sheet.worksheet(ws_name)
    df = pd.DataFrame(
        columns=ws.get_all_values()[0],
        data=ws.get_all_values()[1:], # 第一行為 column
    )
    return df

def connect_to_bigquery():
    '''建立 bigquery api 連線'''
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
    creds = json.loads(Variable.get('gsheet_key'))
    credentials = service_account.Credentials.from_service_account_info(creds, scopes=scopes)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    return client

def upload_data_to_bigquery(dataset, table, df, write_disposition='WRITE_APPEND'):
    '''上傳資料到 bigquery table'''
    client = connect_to_bigquery()
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition=write_disposition  # 可根據需求設定覆寫模式
    table_id = f'{client.project}.{dataset}.{table}'
    job = client.load_table_from_dataframe(dataframe=df, destination=table_id, job_config=job_config)
    job.result()

def query_data_from_bigquery(sql_query):
    '''查詢 bigquery table 的資料'''
    client = connect_to_bigquery()
    query_job = client.query(query=sql_query)
    result_df = query_job.to_dataframe()
    return result_df

def is_data_uploaded(dataset, table, date):
    sql_query = f"""
        SELECT count(*) as counts
        FROM `{dataset}.{table}` 
        WHERE Date = '{date}'
    """
    result_df = query_data_from_bigquery(sql_query)
    
    result = result_df.loc[0, 'counts']
    if result == 0:
        return False
    else:
        return True
