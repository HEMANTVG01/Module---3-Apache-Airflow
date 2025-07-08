from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'Hemant',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

config = {
    'dag_id_01': {'schedule': '*/15 * * * *', 'start_date': datetime(2025, 7, 2)},
    'dag_id_02': {'schedule': '*/15 * * * *', 'start_date': datetime(2025, 7, 2)},
    'dag_id_03': {'schedule': '*/15 * * * *', 'start_date': datetime(2025, 7, 2)}
}

dags = {}

def check_table_exists():
    return 'insert_row'  

def print_process_start():
    print("Pipeline started. Checking for table...")

def create_table():
    print("Creating table...")

def insert_row():
    print("Inserting a new row...")

for dag_id, dag_conf in config.items():
    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule=dag_conf['schedule'],
        start_date=dag_conf['start_date'],
        catchup=False
    ) as dag:

        start_task = PythonOperator(
            task_id='print_process_start',
            python_callable=print_process_start
        )
        bash_task = BashOperator(
            task_id = 'check_who_am_I',
            bash_command = 'whoami'
        )

        branch_task = BranchPythonOperator(
            task_id='check_table_exists',
            python_callable=check_table_exists
        )

        create_table_task = PythonOperator(
            task_id='create_table',
            python_callable=create_table
        )

        insert_row_task = PythonOperator(
            task_id='insert_row',
            python_callable=insert_row
        )

        query_task = EmptyOperator(
            task_id='query_table',
            trigger_rule=TriggerRule.NONE_FAILED
        )

        start_task >> branch_task
        branch_task >> [create_table_task, insert_row_task]
        [create_table_task, insert_row_task] >> query_task

        dags[dag_id] = dag

globals().update(dags)
