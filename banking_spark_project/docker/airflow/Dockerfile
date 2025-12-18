FROM apache/airflow:2.7.1-python3.9

USER root

# Установка Java и утилит
RUN apt-get update && \
    apt-get install -y default-jdk procps build-essential && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Драйвер Postgres
RUN curl -o /home/airflow/postgresql-42.2.18.jar https://jdbc.postgresql.org/download/postgresql-42.2.18.jar