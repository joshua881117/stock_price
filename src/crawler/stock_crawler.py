import datetime
import time
import typing

import pandas as pd
import requests
import logging


def is_weekend(day: int) -> bool:
    '''判斷是否為週末'''
    return day in [5, 6]

def clear_data(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """資料清理, 將文字轉成數字"""
    for col in ["TradeVolume", "Transaction", "TradeValue", "Open", "Max", "Min", "Close", "Change"]:
        # 將格式去除，未有資料或是除權息改為 0 
        df[col] = (
            df[col].astype(str).str.replace(",", "").str.replace("X", "").str.replace("+", "") \
            .str.replace("----", "0").str.replace("---", "0").str.replace("--", "0").str.replace(" ", "") \
            .str.replace("除權息", "0").str.replace("除息", "0").str.replace("除權", "0")
        )
    return df

def colname_to_eng(
    df: pd.DataFrame,
    colname: typing.List[str],
) -> pd.DataFrame:
    """資料欄位轉換, 英文有助於接下來存入資料庫"""
    # 不需要的欄位就改為空白
    taiwan_stock_price = {
        "證券代號": "StockID",
        "證券名稱": "",
        "成交股數": "TradeVolume",
        "成交筆數": "Transaction",
        "成交金額": "TradeValue",
        "開盤價": "Open",
        "最高價": "Max",
        "最低價": "Min",
        "收盤價": "Close",
        "漲跌(+/-)": "Dir",
        "漲跌價差": "Change",
        "最後揭示買價": "",
        "最後揭示買量": "",
        "最後揭示賣價": "",
        "最後揭示賣量": "",
        "本益比": ""
    }
    df.columns = [taiwan_stock_price[col] for col in colname]
    df = df.drop([""], axis=1) # 拿掉不需要的欄位
    return df
    
def convert_change(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """漲跌幅欄位資料處理"""
    logging.info("convert_change")
    df["Dir"] = (df["Dir"].str.split(">").str[1].str.split("<").str[0]) # 抓取 <> 中的資料
    df["Change"] = (df["Dir"] + df["Change"]) # dir 為正負號
    df["Change"] = (df["Change"].str.replace(" ", "").str.replace("X", "").astype(float))
    df = df.fillna("")
    df = df.drop(["Dir"], axis=1)
    return df

def twse_header():
    """網頁瀏覽時, 所帶的 request header 參數, 模仿瀏覽器發送 request"""
    return {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Host": "www.twse.com.tw",
        "Referer": "https://www.twse.com.tw/zh/page/trading/exchange/MI_INDEX.html",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }

def crawler_twse(
    date: str,
) -> pd.DataFrame:
    """
    證交所網址
    https://www.twse.com.tw/zh/rwd/zh/afterTrading/MI_INDEX.html
    """
    logging.info("crawler_twse")
    # headers 中的 Request url
    url = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={date}&type=ALL&response=json"
    url = url.format(date=date.replace("-", ""))
    # 避免被證交所 ban ip, 在每次爬蟲時, 先 sleep 5 秒
    time.sleep(5)
    # request method
    res = requests.get(url, headers=twse_header())
    # 2009 年以後的資料, 股價在 response 中的 data9
    # 2009 年以後的資料, 股價在 response 中的 data8
    df = pd.DataFrame()
    try:
        if "tables" in res.json():
            df = pd.DataFrame(res.json()["tables"][8]['data'])
            colname = res.json()["tables"][8]['fields']
        elif res.json()["stat"] in [
            "查詢日期小於93年2月11日，請重新查詢!",
            "很抱歉，沒有符合條件的資料!",
        ]:
            pass
    except Exception as e:
        logging.error(e)
        return pd.DataFrame()

    if len(df) == 0:
        return pd.DataFrame()
    # 欄位中英轉換
    df = colname_to_eng(df.copy(), colname)
    df["Date"] = date
    df = convert_change(df.copy())
    df = clear_data(df.copy())
    return df





