#sudo mkdir -p /home/project/airflow/dags/finalassignment/staging
#sudo chmod -R 777 /home/project/airflow/dags/finalassignment
#sudo curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz -o /home/project/airflow/dags/finalassignment/tolldata.tgz


# import the libraries
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


#defining DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'email': ['latypov.oleksii.la@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



# defining the DAG
dag = DAG(
    'ETL_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='Airflow Final Assignment',
)

#define task unzip_data BashOperator
unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command = 'tar -xzvf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment',
    dag=dag

)

#define task extract_data_from_csv

extract_data_from_csv=BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = 'cut -d "," -f1-4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv',
    dag=dag
)

# define task extract_data_from_tsv

extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = 'cut -f5-7 < /home/project/airflow/dags/finalassignment/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/tsv_data.csv',
    dag=dag
)


# define task extract_data_from_fixed_width

extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = 'cut -c 59- < /home/project/airflow/dags/finalassignment/payment-data.txt > /home/project/airflow/dags/finalassignment/fixed_width_data.csv',
    dag=dag
)


# define task consolidate_data

consolidate_data = BashOperator(
    task_id= 'consolidate_data',
    bash_command='paste -d "," /home/project/airflow/dags/finalassignment/csv_data.csv \
    /home/project/airflow/dags/finalassignment/tsv_data.csv \
    /home/project/airflow/dags/finalassignment/fixed_width_data.csv \
    > /home/project/airflow/dags/finalassignment/extracted_data.csv',
    dag= dag,
)

# define task transform_data

transform_data = BashOperator(
    task_id= 'transform_data',
    bash_command= 'tr "[a-z]" "[A-Z]" < /home/project/airflow/dags/finalassignment/extracted_data.csv',
    dag= dag,
)



unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data