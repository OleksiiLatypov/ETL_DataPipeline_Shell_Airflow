# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'Oleksii',
    'start_date': days_ago(0),
    'email': ['latypov.oleksii.la@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#define DAG

dag = DAG(
    'dummy_dag',
    default_args=default_args,
    description='DUMMY DAG',
    schedule_interval=timedelta(minutes=1),
)



#define tasks

task1 = BashOperator(
    task_id = 'task1',
    bash_command='sleep 1',
    dag=dag,
)

task2 = BashOperator(
    task_id = 'task2',
    bash_command='sleep 2',
    dag=dag,
)


task3 = BashOperator(
    task_id='task3',
    bash_command='sleep 3',
    dag=dag,

)


# task Pipeline
#cool command to check what is wrong
#!!!!
# airflow dags list-import-errors

task1 >> task2 >> task3