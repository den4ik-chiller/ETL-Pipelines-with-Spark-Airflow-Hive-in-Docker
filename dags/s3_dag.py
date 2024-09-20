# Создание кластера в dataproc https://yandex.cloud/ru/docs/data-proc/operations/cluster-create
# Настройка для airflow https://yandex.cloud/ru/docs/data-proc/tutorials/airflow-automation#infra


from airflow import DAG, settings
from airflow.models import Connection, Variable
from airflow.utils.dates import days_ago
from airflow.providers.yandex.operators.yandexcloud_dataproc import DataprocCreatePysparkJobOperator

YC_SOURCE_BUCKET = 'pyspark-buck'

default_args = {
    'owner': 'Denis',
    'start_date': days_ago(2),
    'depends_on_past': False
}

session = settings.Session()
ycS3_connection = Connection(
    conn_id='yc-s3'
)
if not session.query(Connection).filter(Connection.conn_id == ycS3_connection.conn_id).first():
    session.add(ycS3_connection)
    session.commit()

ycSA_connection = Connection(
    conn_id='yc-airflow-sa'
)
if not session.query(Connection).filter(Connection.conn_id == ycSA_connection.conn_id).first():
    session.add(ycSA_connection)    
    session.commit()

with DAG('Spark_Submit_dag_s3', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    spark_task = DataprocCreatePysparkJobOperator(
        task_id = 'SparkJob',
        main_python_file_uri =f's3a://{YC_SOURCE_BUCKET}/scripts/create-table.py',
        cluster_id = 'c9q5qhti18271q0pfqva',
        connection_id=ycSA_connection.conn_id,
        properties = {
            'spark.submit.master': 'yarn',
            'spark.submit.deployMode': 'client'
        },
        dag=dag
    )
spark_task
