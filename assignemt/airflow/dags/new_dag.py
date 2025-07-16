#sudo mkdir -p /home/project/airflow/dags/finalassignment/staging
#sudo chmod -R 777 /home/project/airflow/dags/finalassignment
#sudo curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz -o /home/project/airflow/dags/finalassignment/tolldata.tgz


# import the libraries
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


# 3. Define the DAG arguments as per the following details in the ETL_toll_data.py file:
default_args = {
    'owner': 'Oleksii',
    'start_date': days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



# 4. Define the DAG in the ETL_toll_data.py file using the following details.
dag = DAG(
    'ETL_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow Final Assignment',
)


# Create a task named unzip_data to unzip data
unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command = 'tar -xzvf /home/project/airflow/dags/finalassignment/tolldata.tgz\
     -C /home/project/airflow/dags/finalassignment',
    dag=dag

)

# Create a task named extract_data_from_csv to extract the fields Rowid,\
# Timestamp, Anonymized Vehicle number, and Vehicle type from the vehicle-data.csv\
# file and save them into a file named csv_data.csv

extract_data_from_csv=BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command ='cut -d"," -f1-4 /home/project/airflow/dags/finalassignment/vehicle-data.csv >\
     /home/project/airflow/dags/finalassignment/csv_data.csv',
    dag=dag
)

# Create a task named extract_data_from_tsv to extract the fields Number of axles, Tollplaza id,\
# and Tollplaza code from the tollplaza-data.tsv file and save it into a file named tsv_data.csv

extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = 'tr "\t" "," < /home/project/airflow/dags/finalassignment/tollplaza-data.tsv | cut -d"," -f5,6,7 >\
     /home/project/airflow/dags/finalassignment/tsv_data.csv',
    dag=dag
)


# Create a task named extract_data_from_fixed_width to extract the fields Type of Payment code, and Vehicle\
# Code from the fixed width file payment-data.txt and save it into a file named fixed_width_data.csv

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="""
    awk '{printf "%s,%s\\n", substr($0,59,3), substr($0,62,6)}' \
    /home/project/airflow/dags/finalassignment/payment-data.txt \
    > /home/project/airflow/dags/finalassignment/fixed_width_data.csv
    """,
    dag=dag
)






# Create a task named consolidate_data to consolidate data extracted from previous tasks.\
# This task should create a single csv file named extracted_data.csv by combining data from the following files:

consolidate_data = BashOperator(
    task_id= 'consolidate_data',
    bash_command='paste -d"," /home/project/airflow/dags/finalassignment/csv_data.csv\
      /home/project/airflow/dags/finalassignment/tsv_data.csv\
      /home/project/airflow/dags/finalassignment/fixed_width_data.csv >\
      /home/project/airflow/dags/finalassignment/extracted_data.csv',
    dag= dag,
)

# define task transform_data

# transform_data = BashOperator(
#     task_id='transform_data',
#     bash_command="""
#     cut -d',' -f1-3 /home/project/airflow/dags/finalassignment/extracted_data.csv > /tmp/left.csv && \
#     cut -d',' -f4 /home/project/airflow/dags/finalassignment/extracted_data.csv | tr '[:lower:]' '[:upper:]' > /tmp/vehicle_type.csv && \
#     cut -d',' -f5-9 /home/project/airflow/dags/finalassignment/extracted_data.csv > /tmp/right.csv && \
#     paste -d',' /tmp/left.csv /tmp/vehicle_type.csv /tmp/right.csv > /home/project/airflow/dags/finalassignment/transformed_data.csv
#     """,
#     dag=dag
# )


# transform_data = BashOperator(
#     task_id='transform_data',
#     bash_command="tr '[:lower:]' '[:upper:]' < /home/project/airflow/dags/finalassignment/extracted_data.csv >\
#      /home/project/airflow/dags/finalassignment/transformed_data.csv",
#     dag=dag
# )

transform_data = BashOperator(
    task_id='transform_data',
    bash_command="""
    SEP=","
    IN="/home/project/airflow/dags/finalassignment/extracted_data.csv"
    OUT="/home/project/airflow/dags/finalassignment/transformed_data.csv"

    paste -d$SEP \
      <(cut -d$SEP -f1-3 "$IN") \
      <(cut -d$SEP -f4 "$IN" | tr '[:lower:]' '[:upper:]') \
      <(cut -d$SEP -f5- "$IN") > "$OUT"
    """,
    dag=dag,
)

clean_data = BashOperator(
    task_id='clean_data',
    bash_command="""paste -d "," - - < /home/project/airflow/dags/finalassignment/transformed_data.csv > /home/project/airflow/dags/finalassignment/cleaned_data.csv
""",
    dag=dag,
)





unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data >> clean_data