from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'capstone',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

dag1 =  DAG(
    dag_id = 'dag_id_1',
    default_args = default_args,
    start_date = datetime(2025, 6, 30),
    schedule = '*/15 * * * *'
)

dag2 = DAG(
    dag_id = 'dag_id_2',
    default_args = default_args,
    start_date = datetime(2025, 6, 30),
    schedule = '*/15 * * * *'
)

dag3 = DAG(
    dag_id = 'dag_id_3',
    default_args = default_args,
    start_date = datetime(2025, 6, 30),
    schedule = '*/15 * * * *'
)
