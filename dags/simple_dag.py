from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import random


default_args = {
    'owner': 'Denis',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

def generate_random_numbers(**context):
    numbers = [random.randint(0, 100) for _ in range(10)]

    context['ti'].xcom_push(key='random_num', value=numbers)


def find_min_max(**context):
    numbers = context['ti'].xcom_pull(key='random_num', task_ids='generate_random_numbers')

    max_min_dict = {'max': max(numbers), 
                    'min': min(numbers)}
    
    context['ti'].xcom_push(key='max_min', value=max_min_dict)
    print(f"Минимальное значение - {max_min_dict['min']}, \
          Максимальное значение - {max_min_dict['max']}")
    
    return f"Минимальное значение - {max_min_dict['min']}, \
          Максимальное значение - {max_min_dict['max']}"



with DAG(
    dag_id = 'max_min_dag',
    default_args = default_args,
    description = 'DAG определяющий max and min',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    start = EmptyOperator(
        task_id='start'
    )

    generate_random_numbers_task = PythonOperator(
        task_id='generate_random_numbers',
        python_callable=generate_random_numbers, 
        provide_context=True
    )

    find_min_max_task = PythonOperator(
        task_id='find_min_max',
        python_callable=find_min_max,
        provide_context=True
    )

    bash_task = BashOperator(
        task_id='bash_task',
         bash_command='echo "{{ti.xcom_pull(task_ids=\'find_min_max\')}}"'
    )

    finish = EmptyOperator( 
        task_id = 'finish'
    )

    start >> generate_random_numbers_task >> find_min_max_task >> bash_task >> finish