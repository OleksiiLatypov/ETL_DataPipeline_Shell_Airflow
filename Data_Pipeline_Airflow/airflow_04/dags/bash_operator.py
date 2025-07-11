from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# DAG default arguments
default_args = {
    'owner': 'Oleksii',
    'start_date': days_ago(0),
    'email': ['latypov.oleksii.la@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'ETL_Server_Access_Log_Processing',
    default_args=default_args,
    description='My first DAG',
    schedule_interval=timedelta(days=1),
)

# Define file paths
log_path = '/home/project/airflow/dags'
log_file = f'{log_path}/web-server-access-log.txt'
extracted_file = f'{log_path}/extracted.txt'
capitalized_file = f'{log_path}/capitalized.txt'
zip_file = f'{log_path}/log.zip'

# Task: Download the log file
download = BashOperator(
    task_id='download',
    bash_command=(
        f'echo "Downloading log file..." && '
        f'curl "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/'
        f'IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/'
        f'web-server-access-log.txt" -o {log_file} && '
        f'ls -l {log_path}'
    ),
    dag=dag,
)

# Task: Extract timestamp and visitor ID
extract = BashOperator(
    task_id='extract',
    bash_command=(
        f'echo "Extracting fields..." && '
        f'cut -f1,4 -d"#" {log_file} > {extracted_file} && '
        f'ls -l {log_path}'
    ),
    dag=dag,
)

# Task: Transform to uppercase
transform = BashOperator(
    task_id='transform',
    bash_command=(
        f'echo "Transforming to uppercase..." && '
        f'tr "[a-z]" "[A-Z]" < {extracted_file} > {capitalized_file} && '
        f'ls -l {log_path}'
    ),
    dag=dag,
)

# Task: Load (zip the transformed file)
load = BashOperator(
    task_id='load',
    bash_command='zip log.zip capitalized.txt',
    dag=dag,
)

# Task pipeline
download >> extract >> transform >> load
