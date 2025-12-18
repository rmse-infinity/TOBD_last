from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'bank_bigdata_pipeline',
    default_args=default_args,
    description='HDFS + Spark + Postgres Pipeline',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    upload_to_hdfs = BashOperator(
        task_id='upload_to_hdfs',
        bash_command="""
            hdfs dfs -fs hdfs://namenode:9000 -mkdir -p /user/data/
            hdfs dfs -fs hdfs://namenode:9000 -rm -f /user/data/transactions.parquet
            hdfs dfs -fs hdfs://namenode:9000 -put /opt/airflow/data/train_1.parquet /user/data/transactions.parquet
            echo "Upload complete"
        """
    )

    run_spark_job = BashOperator(
        task_id='run_spark_job',
        bash_command="""
            export JAVA_HOME=/usr/lib/jvm/default-java
            spark-submit \
            --master local[*] \
            --driver-class-path /home/airflow/postgresql-42.2.18.jar \
            --jars /home/airflow/postgresql-42.2.18.jar \
            /opt/airflow/spark_jobs/process.py
        """
    )

    upload_to_hdfs >> run_spark_job