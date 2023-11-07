# 基於 airflow 設定檔
FROM apache/airflow:2.7.2 
# 將 container 目錄設為 /
WORKDIR /

# 將本地端的檔案複製到 container 中
COPY ./requirements.txt /requirements.txt

# 升級 pip 到最新版本並安裝在 requirements.txt 的套件
RUN pip install --user --upgrade pip
RUN pip install -r requirements.txt