from airflow import DAG
from datetime import datetime
from datetime import timedelta
from twitter_etl import run_twitter_etl
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    "twitter_etl", default_args=default_args,
    schedule_interval=timedelta(days=1,
    catchup=False)) as dag:

    start_process = DummyOperator(
        task_id='start_processing',
        dag=dag
    )

    run_etl = PythonOperator(
        task_id='complete_twitter_etl',
        python_callable=run_twitter_etl,
        dag=dag
    )

    end_process = DummyOperator(
        task_id='end_processing',
        dag=dag
    )

start_process >> run_etl >> end_process
