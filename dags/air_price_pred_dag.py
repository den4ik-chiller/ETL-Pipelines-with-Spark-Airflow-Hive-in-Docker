from airflow import DAG
from datetime import datetime, timedelta
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
    dag_id = 'airbnb_model',
    default_args = default_args,
    description = 'Обработка датасета airbnb, обучение модели и получение предсказаний',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    start = EmptyOperator(
        task_id='start'
    )

    air_data_analysis = SparkSubmitOperator(
        task_id='data_analysis',
        application='/opt/airflow/dags/air_data_analysis.py', 
        verbose=True,
        conn_id='spark_default',
        conf={"spark.master": "spark://spark-master:7077"}, 
    )

    air_train_model = SparkSubmitOperator(
        task_id='train_model',
        application='/opt/airflow/dags/air_train_model.py', 
        verbose=True,
        conn_id='spark_default',
        conf={"spark.master": "spark://spark-master:7077"}, 
    )

    air_pred_model = SparkSubmitOperator(
        task_id='pred_model',
        application='/opt/airflow/dags/air_pred_model.py', 
        verbose=True,
        conn_id='spark_default',
        conf={"spark.master": "spark://spark-master:7077"}, 
    )

    finish = EmptyOperator( 
        task_id = 'finish'
    )

    # Определение последовательности задач в DAG
    start >> air_data_analysis >> air_train_model >> air_pred_model >> finish
