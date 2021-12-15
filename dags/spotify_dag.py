from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021,11,27),
    'email': 'contatocesarino@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'spotfy_dag',
    default_args = default_args,
    description="Sample dag",
    schedule_interval= timedelta(days=1),
)

def just_a_function():
    print('testeeee')

run_etl = PythonOperator(
    task_id= 'spotify_etl',
    python_callable=just_a_function,
    dag=dag
)

run_etl


