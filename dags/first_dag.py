from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'Denis',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

def fetch_data(**context):
    data = {'name': 'Denis', 
            'role': 'Data Engineer'}
    
    context['ti'].xcom_push(key='data', value = data)

def process_data(**context):
    data = context['ti'].xcom_pull(key='data', task_ids = 'fetch_data')
    processed_data = f'Processing {data['name']} and role - {data['role']}'

    print(processed_data)
    return processed_data


with DAG(
    dag_id = 'simple_example_dag',
    default_args = default_args,
    description = 'Первый DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    start = EmptyOperator(
        task_id = 'start'
        )
    
    fetch_data_task = PythonOperator(
        task_id = 'fetch_data',
        python_callable=fetch_data,
        provide_context=True
    )

    process_data_task = PythonOperator(
        task_id = 'process_data',
        python_callable=process_data,
        provide_context = True
    )

    bash_task = BashOperator(
        task_id = 'bash_task',
        bash_command='echo "{{ti.xcom_pull(task_ids=\'process_data\')}}"'
    )

    finish = EmptyOperator(
        task_id = 'finish'
    )

    start >> fetch_data_task >> process_data_task >> bash_task >> finish