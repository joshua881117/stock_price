import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from airflow.models import Variable
import os
import json
def generate_message(ID_list):
    '''產生要傳送到 slack 的訊息'''
    n = len(ID_list)
    message = f"今日有{n}支股票超越目標價格，股票代碼為 " + ", ".join(ID_list)
    return message

def check_target_stock_price(stock_df, target_df):
    '''確認今日價格是否有超越目標價格'''
    ID_list = []
    result = pd.DataFrame()
    for index, row in target_df.iterrows():
        stockID = row['stockID']
        target_price = row['target_price']
        stock_price = stock_df[stock_df['StockID'] == stockID]['Close'].values[0]
        if float(stock_price) > float(target_price):
            ID_list.append(stockID)
            data = {'stockID':stockID, 'target_price':target_price, 'now_price':stock_price}
            r = pd.DataFrame(data=data, index=[0])
            result = pd.concat([result, r])
    return ID_list, result

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
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds, scopes=scopes)
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
