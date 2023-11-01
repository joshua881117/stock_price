FROM apache/airflow:2.7.2
WORKDIR /
COPY ./requirements.txt /requirements.txt
COPY src /src

RUN pip install --user --upgrade pip
RUN pip install -r requirements.txt
RUN pip install pymysql