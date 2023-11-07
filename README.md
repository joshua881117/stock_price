# 股票到價通知

## 專案內容
開發一個每日爬取證交所股價資料爬蟲 + 當股價到達目標漲跌幅時會傳訊息給使用者的 Slack APP

## 開發工具
- Docker：用於架設 Airflow
- Airflow：實現每日定時爬蟲 + 確認股價是否達目標漲跌幅
- BigQuery：雲端資料倉儲

## 專案架構

### Airflow
- 用 Docker 在本地端架設 Airflow
- 因使用 LocalExecutor，不需要 Worker
![](https://gitlab.com/joshualin2/airflow/-/blob/ef4a3a837d02841a4877ead73f14b6ddcaeb9ed7/pictures/Airflow%20%E6%9E%B6%E6%A7%8B.png)

### 工作流
股價爬蟲
圖片：爬蟲 DAG

股票到價通知
圖片：通知 DAG

## 實作

### Airflow
##### 環境架設
用 Docker 架設 Airflow 
- 到 [Airflow 官網](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) 下載 docker-compose.yml
- 修改 docker-compose.yml
    - 將 Executor 由 CeleryExecutor 改為 LocalExecutor
        - 因LocalExecutor 在本地端執行 Dag 所以不需要用到 Worker, Redis（註解掉架設 Worker, Redis 部分）
    - 調整 config
        ```
        # 新增時區設定
        AIRFLOW__CORE__DEFAULT_TIMEZONE: Asia/Taipei
        AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE: Asia/Taipei

        # 將 executor 改為 local executor
        AIRFLOW__CORE__EXECUTOR: LocalExecutor

        # 資料庫改為自行架設的 mysql 資料庫
        AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: mysql+pymysql://airflow:airflow@mysql:3306/airflow
        AIRFLOW__CORE__SQL_ALCHEMY_CONN: mysql+pymysql://airflow:airflow@mysql:3306/airflow

        # 註解掉載入 DAG example 的設定
        # AIRFLOW__CORE__LOAD_EXAMPLES: 'true'

        # 設定 email，後續 DAG 出錯時會寄信通知
        AIRFLOW__EMAIL__EMAIL_BACKEND: airflow.utils.email.send_email_smtp
        AIRFLOW__SMTP__SMTP_HOST: smtp.gmail.com
        AIRFLOW__SMTP__SMTP_STARTTLS: True
        AIRFLOW__SMTP__SMTP_SSL: False
        AIRFLOW__SMTP__SMTP_USER: your_email@gmail.com
        AIRFLOW__SMTP__SMTP_PASSWORD: your_password # 要到 Gmail 設定應用程式密碼
        AIRFLOW__SMTP__SMTP_PORT: 587
        AIRFLOW__SMTP__SMTP_MAIL_FROM: your_email@gmail.com
        ```
        
    - volume 新增 data, src 資料夾
        ```
        volumes:
            - ${AIRFLOW_PROJ_DIR:-.}/src:/opt/airflow/src # Dag 會用到的 functions
            - ${AIRFLOW_PROJ_DIR:-.}/data:/opt/airflow/data # 暫存數據檔案
        ```
    - 安裝 Python 套件
        - 新增 requirements.txt, Dockerfile
            - requirements.txt ：python 用到的套件
                ```
                # 僅列出部分用到套件
                aiohttp==3.8.5
                aiosignal==1.3.1
                alembic==1.12.0
                annotated-types==0.5.0
                anyio==4.0.0
                apache-airflow==2.7.1
                apache-airflow-providers-common-sql==1.7.1
                apache-airflow-providers-ftp==3.5.1
                apache-airflow-providers-http==4.5.1
                apache-airflow-providers-imap==3.3.1
                apache-airflow-providers-sqlite==3.4.3
                ```
            - Dockerfile
                ```
                # 基於 airflow 設定檔
                FROM apache/airflow:2.7.2 
                # 將 container 目錄設為 /
                WORKDIR /

                # 將本地端的檔案複製到 container 中
                COPY ./requirements.txt /requirements.txt

                # 升級 pip 到最新版本並安裝在 requirements.txt 的套件
                RUN pip install --user --upgrade pip
                RUN pip install -r requirements.txt
                ```
        - 更改 docker-compose.yml 
            ```
            x-airflow-common:
            &airflow-common
            image: joshua881117/extending_airflow:${GIT_TAG} # 改為用 Dockerfile 建立的 image 名稱，GIT_TAG 後續 CI/CD 會用到
            ```
- 部署 Airflow
    ```
    # 建立 image，版本號先設為 0.0
    docker build -f Dockerfile -t joshua881117/extending_airflow:0.0 .
    # 初始化 airflow database
    docker compose up airflow-init
    # 部署 airflow 在本地端，並在背景執行
    GIT_TAG=0.0 docker compose up -d 
    ```
    - 確認資料夾都有確實掛載進 Container
        ```
        # 列出目前運行的 container，找出 scheduler 的 container ID
        docker ps 
        # 在運行中的 Docker 容器中啟動 bash shell
        docker exec -it <container ID> bash
        ```
    - CI/CD 自動化部署 Airflow
        - 建立 Makefile
            ```
            GIT_TAG := $(shell git describe --abbrev=0 --tags) # 獲取 git repository 最新的版本號

            build-image:
                docker build -f Dockerfile -t joshua881117/extending_airflow:${GIT_TAG} .

            push-image:
                docker push joshua881117/extending_airflow:${GIT_TAG}

            deploy-airflow:
                GIT_TAG=${GIT_TAG} docker compose up -d

            ```
        - 建立 .gitlab-ci.yml
            ```
            stages:
            - build
            - deploy

            build-docker-image:
            stage: build
            image: docker
            services:
                - docker:dind # Docker in Docker 允許在一個容器中運行 Docker，用於構建和測試容器
            before_script:
                - docker login -u "joshua881117" -p ${DOCKER_HUB_TOKEN} # docker hub token 存在 gitlab 上
            script:
                - make build-image # 建立 image
                - make push-image # 將 image push 到 docker hub
            tags:
                - build_airflow_image # gitlab runner 標籤
            only:
                - tags # 當下版本號時進行

            deploy-airflow-service:
            stage: deploy
            before_script:
                - docker compose down # 將運行中的服務中止
            script:
                - make deploy-airflow # 部署新版本 airflow
            tags:
                - build_airflow_image # gitlab runner 標籤
            only:
                - tags # 當下版本號時進行
            ```
        - 在 GitLab 建立專案並將程式碼上傳
        - 在 GitLab 設定 Runner
            - 到 CI/CD settings 頁面建立 GitLab Runner
            圖片：GitLab CI/CD Setting
            選擇 Linux，Tags 跟上面 yaml 檔的 tags 要一致(build_airflow_image)
            圖片：GitLab Runner 設定
            - 到本地端安裝 gitlab-runner
                ```
                # 安裝 binary 檔
                sudo curl --output /usr/local/bin/gitlab-runner https://gitlab-runner-downloads.s3.amazonaws.com/latest/binaries/gitlab-runner-darwin-arm64
                sudo curl --output /usr/local/bin/gitlab-runner https://gitlab-runner-downloads.s3.amazonaws.com/latest/binaries/gitlab-runner-darwin-arm64

                # 給予運行權限
                sudo chmod +x /usr/local/bin/gitlab-runner

                # 註冊 gitlab runner
                cd ~
                gitlab-runner install
                gitlab-runner start
                # 相關設定
                # --url "https://gitlab.com/"
                # --executor "shell"
                # --tag "build_airflow_image"
                ```
        - 測試自動部署流程
            - 下版本號
            圖片：TAG 設定
            - 查看 pipeline 運行是否順利
            圖片：自動化部署 pipeline

### DAG 運用到的 Functions
#### 股價爬蟲
- 找到證交所網站公布每支股票資料的網址
    圖片：證交所網站
- 發送 requests
    ```
    url = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={date}&type=ALL&response=json"
    response = requests.get(url, headers=headers)
    ```
- 回傳的資料為 json 檔，需要的資料放在第 9 個 table
    ```
    df = pd.DataFrame(res.json()["tables"][8]['data'])
    colname = res.json()["tables"][8]['fields'] 
    ```
- 資料清洗+欄位轉換
#### 上傳資料至 BigQuery 
- 開啟 BigQuery API：到 API 和服務頁面，點選啟用 API 和服務，並啟用 BigQuery API
    圖片：API 和服務頁面
    圖片：BigQuery API
- 建立服務帳戶：到 IAM 與管理的服務帳戶頁面，點選建立服務帳戶，建立帳戶並授予 BigQuery 管理員權限
    圖片：服務帳戶頁面
    圖片：建立服務帳戶
    圖片：給予權限
- 建立金鑰：到服務帳戶清單點選剛建立的服務帳戶，進到金鑰頁面建立金鑰
    圖片：服務帳戶清單
    圖片：金鑰頁面
- 將金鑰下載並存放在 stock_app 目錄(和建立 API 連線的 python 檔同個目錄)
- 測試是否能上傳資料至 BQ
    ```
    from google.cloud import bigquery
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
    key_path = 'credentials.json'
    credentials = service_account.Credentials.from_service_account_file(key_path, scopes=scopes)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    table_id = 'project_id.dataset_id.table_id'
    client.load_table_from_dataframe(df, table_id)
    ```
#### 傳送訊息至 Slack
- 到 [Slack API APP 頁面](https://api.slack.com/apps)
- 建立 APP
    圖片：建立 APP
- 到 OAuth & Permissions 頁面複製 APP 金鑰並存在 Airflow Variables
    圖片：APP 金鑰
- 開啟上傳檔案、訊息到 Channel 的權限
    圖片：開啟 Slack 權限
- 將 Slack APP 添加到目標 Channel
    圖片：添加應用
- 用 Python 測試 Slack APP 是否可以發送訊息、檔案
    ```
    oauth_token = "your_app_token"
    client = WebClient(token=oauth_token)
    result = client.files_upload(
            channels="your_channel_id",
            initial_comment="message",
            filename=file_name, # 檔案名稱
            content=file, # 上傳的檔案
    )
    ```

### DAG 實作
#### 股價爬蟲
- 設定 DAG 參數
    ```
    default_args = {
        'owner': 'Joshua Lin',
        'email': ['joshua881117@gmail.com'],
        'email_on_failure': True, # DAG 失敗時會傳 email 通知
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': dt.timedelta(minutes = 5),
    }
    with DAG(
        dag_id = 'stock_data_crawler',
        default_args = default_args,
        description = 'daily stock price crawler',
        start_date = dt.datetime(2023, 1, 1),
        schedule = '0 18 * * Mon-Fri', # 週末不開盤，僅需要每日
        params = {
            "date": Param(str(dt.date.today()), type='string')
        }
    ) as dag:
    ```
- 到證交所爬股價資料
    ```
    def get_data(**kwargs):
        '''爬證交所股票資料'''
        params = kwargs['dag_run'].conf
        # 獲取 DAG 參數，如果未傳入參數則預設為今日
        logical_date = kwargs['dag_run'].logical_date.date()
        if logical_date.weekday() == 4:
            logical_date += dt.timedelta(days=3) # 如果是週一執行的 DAG，logical_date 為前一個執行週期的日期(週五)，因此 logical_date 需要增加 3 天
        else:
            logical_date += dt.timedelta(days=1) # 週二到週四的 logical_date 僅需要加一天
        date = params.get('date', str(logical_date))

        # 爬蟲
        df = crawler_twse(date)
        file_dir = os.path.dirname(__file__) # 目前檔案所在目錄
        file_dir = os.path.abspath(os.path.join(file_dir, os.pardir)) # 上一層目錄
        file_path = os.path.join(file_dir, f'data/stock_data_{date}.csv')
        # 如果爬蟲下來沒資料，代表今天應該未開盤
        if len(df) == 0:
            return 'do_nothing'
        else:
            df.to_csv(file_path, index=False)
            return 'upload_to_db'
    ```
    - logical_date 為上個執行週期的日子，所以週二執行的 DAG logical_date 為週一，週一執行的 DAG logical_date 為週五，但我希望 logical_date 等於執行日期，所以當 logical_date 為週五要加三天，週一到週四會加一天
    - 爬蟲的結果會暫存到 data 資料夾
    - 函式回傳的結果有兩種，分別為兩個不同的 task_id，根據有沒有爬到資料去執行不同的 task

- 上傳股價資料到 BigQuery
    ```
    def upload_data(**kwargs):
        '''上傳股票資料到 BigQuery'''
        params = kwargs['dag_run'].conf
        # 獲取 DAG 參數，如果未傳入參數則預設為今日
        # table = params.get('table', 'TaiwanStockPrice')
        logical_date = kwargs['dag_run'].logical_date.date()
        if logical_date.weekday() == 4:
            logical_date += dt.timedelta(days=3) # 如果是週一執行的 DAG，logical_date 為前一個執行週期的日期(週五)，因此 logical_date 需要增加 3 天
        else:
            logical_date += dt.timedelta(days=1) # 週二到週四的 logical_date 僅需要加一天
        date = params.get('date', str(logical_date))

        file_dir = os.path.dirname(__file__)
        file_dir = os.path.abspath(os.path.join(file_dir, os.pardir))
        file_path = os.path.join(file_dir, f'data/stock_data_{date}.csv')

        df = pd.read_csv(file_path)

        df['Date'] = df['Date'].apply(lambda x: dt.datetime.strptime(str(x), '%Y-%m-%d'))
        # 上傳到 mysql 資料庫
        # r = db.get_db_router()
        # with r.mysql_conn as conn:
        #     db_executor.upload_data(df, table, conn)
        #     if os.path.exists(file_path):
        #         # 刪除檔案
        #         os.remove(file_path)

        # 如果資料已經上傳就不重複上傳
        if is_data_uploaded(dataset='Joshua', table='stock_price', date=date):
            logging.info("Already upload data")
        else:
            upload_data_to_bigquery(dataset='Joshua', table='stock_price', df=df, write_disposition='WRITE_APPEND')
            logging.info(f"Success upload {len(df)} data")
        # 上傳完就刪除 csv 檔
        if os.path.exists(file_path):
            # 刪除檔案
            os.remove(file_path)    
    ```
    - 讀取剛剛暫存在 data 資料夾的股價資料
    - 先判斷今日資料是否已存在 BigQuery，避免重複上傳，如果沒有資料就上傳資料
    - 上傳完後刪除 data 資料夾中的股價
    - 註解掉的部分：原本將資料上傳到 MySQL 資料庫，但想練習串接 BigQuery 且 BigQuery 更方便串接到其他分析工具(像是 Looker Studio)

- 上傳成功發送訊息至 Slack
    ```
    def send_message(**kwargs):
        '''發送成功上傳訊息至 slack'''
        params = kwargs['dag_run'].conf
        logical_date = kwargs['dag_run'].logical_date.date()
        if logical_date.weekday() == 4:
            logical_date += dt.timedelta(days=3) # 如果是週一執行的 DAG，logical_date 為前一個執行週期的日期(週五)，因此 logical_date 需要增加 3 天
        else:
            logical_date += dt.timedelta(days=1) # 週二到週四的 logical_date 僅需要加一天
        date = params.get('date', str(logical_date))
        
        send_message_to_slack(
            channel='#爬蟲通知', 
            app_name='stock_notify',
            message=f'{date} 股票資料上傳成功'
        )
    ```
    - 發送訊息到 slack
        圖片：slack 股價爬蟲

- 組裝 DAG
    ```
    start = EmptyOperator(task_id='start')

    do_nothing = EmptyOperator(task_id='do_nothing')

    get_stock_data = BranchPythonOperator(
        task_id='get_stock_data',
        python_callable=get_data
    )

    upload_to_db = PythonOperator(
        task_id='upload_to_db',
        python_callable=upload_data
    )

    send_success_message = PythonOperator(
        task_id='send_success_message',
        python_callable=send_message
    )

    start >> get_stock_data
    get_stock_data >> [upload_to_db, do_nothing]
    upload_to_db >> send_success_message
    ```
    - start, do_nothing 都是 EmptyOperator，不會做任何事

#### 股票到價通知
- 設定 DAG 參數：與股票爬蟲類

- 從 Google Sheet 獲得股票購買明細
    ```
    def get_buy_price():
        '''獲取股票購買明細'''
        sheet_id = '1emVQoQWeMqpAjfW155i3mWLQ2Cqo0OeQFhH4ZNlRR_w'
        buy_df = read_target_stock_sheet(sheet_id, '股票庫存') # 從 google sheet 讀取明細
        buy_df.rename(
            columns={
                '股票代碼': 'stockID',
                '買入價': 'buyPrice',
                '買入股數': 'buyVolume'
            }, 
            inplace=True
        )
        # 轉換欄位型態
        buy_df['buyPrice'] = pd.to_numeric(buy_df['buyPrice'])
        buy_df['buyVolume'] = pd.to_numeric(buy_df['buyVolume'])
        return buy_df
    ```
    - 使用者需要先在 Google Sheet 打上股票購買紀錄
        圖片：Google Sheet 購買明細

- 計算每支股票平均購買價
    ```
    def calculate_avg_price(ti):
        '''計算每檔股票購買平均價格'''
        buy_df = ti.xcom_pull(task_ids='get_buy_record') # 獲取購買明細
        buy_df['totalValue'] = buy_df.buyPrice * buy_df.buyVolume
        buy_df = buy_df.groupby(by=['stockID']).sum().reset_index()
        buy_df['avgPrice'] = buy_df.totalValue / buy_df.buyVolume
        return buy_df[['stockID', 'avgPrice']]
    ```
    - 獲得購買明細後計算總成本及總股數，並計算每支股票平均購買價
    - ti 代表 task_instance，其中的 Xcom 相關 functions 可以串接 task 的 return 與 input parameters

- 從 BigQuery 撈取今日股價
    ```
    def get_stock_data(ti, **kwargs):
        '''從 BQ 撈取特定股票價格'''
        params = kwargs['dag_run'].conf
        # 獲取 DAG 參數，如果未傳入參數則預設為今日
        logical_date = kwargs['dag_run'].logical_date.date()
        if logical_date.weekday() == 4:
            logical_date += dt.timedelta(days=3) # 如果是週一執行的 DAG，logical_date 為前一個執行週期的日期(週五)，因此 logical_date 需要增加 3 天
        else:
            logical_date += dt.timedelta(days=1) # 週二到週四的 logical_date 僅需要加一天
        date = params.get('date', str(logical_date))

        buy_df = ti.xcom_pull(task_ids='get_avg_price') # 獲取每檔股票平均價格資料
        stockID = list(buy_df['stockID']) # 購買股票代碼清單
        stockID_str = str(stockID).strip('[]')
        # 僅需要查詢有購買的股票代碼
        sql_query = f"""
            SELECT StockID, Close
            FROM Joshua.stock_price
            WHERE Date = '{date}'
                AND stockID in ({stockID_str})
        """
        stock_df = query_data_from_bigquery(sql_query)
        return stock_df
    ```
    - 從 BigQuery 撈取今日股價且出現在股票購買明細的股票代碼

- 確認今日是否有開盤
    ```
    def is_market_opened(ti):
        '''判斷今日是否有開盤'''
        stock_df = ti.xcom_pull(task_ids='get_stock_record') # 獲取 BQ 撈取的股票資料
        # 如果無資料代表今天沒開盤
        if len(stock_df) == 0:
            return 'market_closed'
        else:
            return 'check_stock_price'
    ```
    - 如果未撈取到 BQ 資料代表今天可能沒開盤，會發送 Slack 訊息通知

- 獲得每支股票目標漲跌幅
    ```
    def get_target_price():
        '''獲取目標股票目標漲跌幅'''
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
        # 轉換欄位型態
        target_df['upPct'] = pd.to_numeric(target_df['upPct'])
        target_df['downPct'] = pd.to_numeric(target_df['downPct'])
        return target_df
    ```
    - 使用者需要先在 Google Sheet 打上股票目標漲跌幅
        圖片：Google Sheet 目標漲跌幅

- 傳送休市通知至 Slack
    ```
    def send_market_closed_message():
        '''發送今日休市訊息'''
        send_message_to_slack(
            channel='#股票到價通知',
            app_name='stock_notify',
            message='今日休市'
        )
    ```

- 計算目前漲跌幅
    ```
    def check_price(ti):
        '''確認今日價格是否有達到目標漲跌幅'''
        buy_df = ti.xcom_pull(task_ids='get_avg_price') # 獲取每檔股票平均價資料
        target_df = ti.xcom_pull(task_ids='get_target_record') # 獲取目標漲跌幅
        stock_df = ti.xcom_pull(task_ids='get_stock_record') # 獲取今日股票價格

        file_dir = os.path.dirname(__file__)
        file_dir = os.path.abspath(os.path.join(file_dir, os.pardir)) # 上一層的路徑

        # 獲取上漲、下跌股票清單
        up_list, down_list, result = check_target_stock_price(stock_df, buy_df, target_df)
        result_path = os.path.join(file_dir, 'data/result.csv') # 將結果存為 csv 檔
        result.to_csv(result_path, index=False)
        return up_list, down_list
    ```
    - 讀取前面獲取的股票平均購買價和今日股票價格，計算每支股票的漲跌幅(如果股票數量多，就要將資料存到 data 資料夾，不能用 xcom_pull 傳輸)

- 確認漲跌幅是否有達到目標
    ```
    def is_meet_target(ti):
        '''判斷是否有達到目標漲跌幅'''
        up_list, down_list = ti.xcom_pull(task_ids='check_stock_price') # 獲取達到漲幅、跌幅的股票代碼
        if len(up_list) + len(down_list) == 0:
            return 'do_nothing'
        else:
            return 'send_stock_message'
        ```
        - 如果沒有達到目標漲跌幅的股票，就不做任何事

- 發送結果至 Slack
    ```
    def send_message_and_file(ti):
        '''發送訊息和檔案到 slack'''
        up_list, down_list = ti.xcom_pull(task_ids='check_stock_price') # 獲取達到漲幅、跌幅的股票代碼
        message = generate_stock_message(up_list, down_list) # 產生訊息

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
    ```
    - 發送訊息和結果檔案到 slack
        圖片：slack 股價通知

- 組裝 DAG
    ```
    start = EmptyOperator(task_id='start')

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

    start >> get_buy_record >> get_avg_price >> [get_stock_record, get_target_record]
    get_stock_record >> check_market_opened >> [check_stock_price, market_closed]
    get_target_record >> check_stock_price
    check_stock_price >> stock_meet_target >> [do_nothing, send_stock_message]
    ```
    - 獲取 BQ 股價資料和獲取 Google Sheet 目標漲跌幅為同步進行
    - 當 BQ 沒有股價資料就發送休市通知，有資料才計算漲跌幅
    - 如果沒有股票達到目標漲跌幅就不做事，有才發送訊息

## 參考資料
- Python 大數據專案 X 工程 X 產品 資料工程師的升級攻略：
    - [書籍](https://www.tenlong.com.tw/products/9786267273739?list_name=b-r7-zh_tw)
    - [GitHub](https://github.com/FinMind/FinMindBook)
- [code2j：Airflow Tutorial for Beginners](https://www.youtube.com/watch?v=K9AnJ9_ZAXE&list=PLwFJcsJ61oujAqYpMp1kdUBcPG0sE0QMT&index=1)
- [一段 Airflow 與資料工程的故事：談如何用 Python 追漫畫連載](https://leemeng.tw/a-story-about-airflow-and-data-engineering-using-how-to-use-python-to-catch-up-with-latest-comics-as-an-example.html)
- [ChickenBenny：Airflow-scraping-ETL-tutorial](https://github.com/ChickenBenny/Airflow-scraping-ETL-tutorial)
