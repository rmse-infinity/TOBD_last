FROM apache/airflow:2.7.1-python3.9

USER root

# 1. Используем зеркала Яндекс для стабильности
RUN sed -i 's/deb.debian.org/mirror.yandex.ru/g' /etc/apt/sources.list && \
    sed -i 's/security.debian.org/mirror.yandex.ru/g' /etc/apt/sources.list

# 2. Ставим Java и Curl
RUN apt-get update && \
    apt-get install -y default-jdk procps build-essential curl && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

# !!! ГЛАВНОЕ ИСПРАВЛЕНИЕ !!!
# Явно добавляем путь к локальным бинарникам пользователя airflow в переменную окружения
ENV PATH="/home/airflow/.local/bin:${PATH}"

COPY requirements.txt .

# 3. Ставим зависимости.
# Добавляем apache-airflow==2.7.1 явно, чтобы pip случайно не удалил его при разрешении конфликтов версий
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir apache-airflow==2.7.1 -r requirements.txt

# 4. Качаем драйвер
RUN curl -o /home/airflow/postgresql-42.2.18.jar https://jdbc.postgresql.org/download/postgresql-42.2.18.jar