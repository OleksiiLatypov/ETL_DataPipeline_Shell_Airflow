
from datetime import timedelta
from airflow.models import DAG

from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# Define DAG arguments

default_args = {
    'owner': 'Oleksii',
    'start_date': days_ago(0),
    'email': ['latypov.oleksii.la@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    }


# Define DAG
dag = DAG(
    'my-first-bash-dag',
    default_args = default_args,
    description='My First Bash Operator DAG',
    schedule_interval=timedelta(days=1)
)



# Defina first TASK
extract = BashOperator(
    task_id='extract',
    bash_command='cut -d":" -f1,3,6  /etc/passwd > /home/project/airflow/dags/extracted-data.txt'
    dag=dag
    )


transform = BashOperator(
    task_id='transform',
    bash_command='tr ":" "," < /home/project/airflow/dags/extracted-data.txt > /home/project/airflow/dags/transformed-data.csv',
    dag=dag
)


#task Pipeline

extract>>transform


# 1.Open a terminal and run the command below to set the AIRFLOW_HOME.
# export AIRFLOW_HOME=/home/project/airflow
# echo $AIRFLOW_HOME


# 2. Run the command below to list out all the existing DAGs.
# airflow dags list


# 3. Verify that my-first-bash-dag is a part of the output.
# airflow dags list|grep "my-first-dag"


# 4. Run the command below to list out all the tasks in my-first-dag
# airflow tasks list my-first-dag


