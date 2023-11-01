FROM apache/airflow:2.7.2
WORKDIR /
COPY ./requirements.txt /requirements.txt
COPY src /src
RUN sudo apt-get update && sudo apt-get install python3.11 -y && sudo apt-get install python3-pip -y
RUN pip install --user --upgrade pip
RUN pip install -r requirements.txt
RUN pip install pymysql