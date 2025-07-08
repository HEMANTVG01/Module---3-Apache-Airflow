from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Hemant',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='Trigger_dag',
    default_args=default_args,
    start_date=datetime(2025, 7, 7),
    schedule='@daily',
    catchup=False
) as dag:

    wait_run_file = FileSensor(
        task_id='wait_run_file',
        fs_conn_id='fs_default',
        filepath='/opt/airflow/data/connecture.numbers',
        poke_interval=30,
        timeout=300,
        mode='poke'
    )

    trigger_dag = TriggerDagRunOperator(
        task_id='trigger_dag',
        trigger_dag_id='python_operator',
        wait_for_completion=True,
        reset_dag_run=True
    )

    remove_run_file = BashOperator(
        task_id='remove_run_file',
        bash_command='rm /opt/airflow/data/connecture.numbers'
    )

    wait_run_file >> trigger_dag >> remove_run_file
