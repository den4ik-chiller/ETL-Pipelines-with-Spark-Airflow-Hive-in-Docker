from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'Denis',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id = 'spark_find_makers_dag',
    default_args = default_args,
    description = 'Производители ПК, все модели ПК которых имеются в таблице PC',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    spark_submit_task = SparkSubmitOperator(
        task_id='spark_submit_task',
        application='/opt/airflow/dags/spark_job.py',  # Путь к вашему Spark приложению
        verbose=True,
        conn_id='spark_default',  # Коннект к Spark 
        conf={"spark.master": "spark://spark-master:7077"},  # Задать режим локального Spark
    )

    # Определение последовательности задач в DAG
    spark_submit_task
