from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


PATH = '/home/project/airflow/dags/finalassignment'


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'email': ['latypov.oleksii.la@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ETL_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='Airflow Final Assignment',
)



# Create a task named unzip_data to unzip data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command=f'tar -xzf {PATH}/tolldata.tgz '
                 f'-C {PATH}',
    dag=dag,
)


# Create a task named extract_data_from_csv to extract the fields Rowid,\
# Timestamp, Anonymized Vehicle number, and Vehicle type from the vehicle-data.csv\
# file and save them into a file named csv_data.csv

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command=f'cut -d "," -f1-4 {PATH}/vehicle-data.csv '
                 f'> {PATH}/csv_data.csv',
    dag=dag,
)


# Create a task named extract_data_from_tsv to extract the fields Number of axles, Tollplaza id,\
# and Tollplaza code from the tollplaza-data.tsv file and save it into a file named tsv_data.csv

# gsub(REGEX, REPLACEMENT, TARGET)
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command=f"cut -f5-7 {PATH}/tollplaza-data.tsv | "
                 "awk '{gsub(/\\r/, \"\"); print}' | tr '\\t' ',' "
                 f"> {PATH}/tsv_data.csv",
    dag=dag,
)


# Create a task named extract_data_from_fixed_width to extract the fields Type of Payment code, and Vehicle\
# Code from the fixed width file payment-data.txt and save it into a file named fixed_width_data.csv

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command=f"cut -c 59- {PATH}/payment-data.txt | "
                 "awk '{$1=$1; gsub(/[ ]+/, \",\"); print}' "
                 f"> {PATH}/fixed_width_data.csv",
    dag=dag,
)


# Create a task named consolidate_data to consolidate data extracted from previous tasks.\
# This task should create a single csv file named extracted_data.csv by combining data from the following files:

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste -d "," '
                 f'{PATH}/csv_data.csv '
                 f'{PATH}/tsv_data.csv '
                 f'{PATH}/fixed_width_data.csv '
                 f'> {PATH}/extracted_data.csv',
    dag=dag,
)


#Create a task named transform_data to transform the vehicle_type field in extracted_data.csv\
# into capital letters and save it into a file named transformed_data.csv in the staging directory.

transform_data = BashOperator(
    task_id='transform_data',
    bash_command='sed \'s/[^,]*/\\U&/4\' '
                 f'{PATH}/extracted_data.csv '
                 f'> {PATH}/staging/transformed_data.csv',
    dag=dag,
)


# Defining task dependencies
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
