import pandas as pd 
import tarfile
import os
import requests


url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
source_file = 'tolldata.tgz'
source_dir = '/workspaces/ETL_DataPipeline_Shell_Airflow/python_finalassignment/dags/python_etl/'
destination_dir =  "/workspaces/ETL_DataPipeline_Shell_Airflow/python_finalassignment/dags/python_etl/staging"

#/workspaces/ETL_DataPipeline_Shell_Airflow/python_finalassignment/dags/python_etl/staging/tolldata.tgz
def download_dataset(url, destination):

    filename = "tolldata.tgz"
    os.makedirs(destination, exist_ok=True)
    
    file_path = os.path.join(destination, filename)

    response = requests.get(url)
    with open(file_path, 'wb') as f:
        f.write(response.content)
    
    print(f"Downloaded dataset to {file_path}")



def unzip_tolldata(source: str, file: str):
    """
    Extracts the contents of the source dataset .tgz file to the specified
    destination directory.

    Args:
        source (str): Path to the source .tgz file.
        destination (str): Directory where the contents will be extracted.
    """
    try:
        with tarfile.open(f'{source}/{file}', "r:gz") as tgz:
            tgz.extractall(source)
            print('Unzip Successfully!')
    except Exception as e:
        print(f"Error extracting {source}: {e}")
    


def extract_data_from_csv(source: str, file: str, destination_dir: str):
    df = pd.read_csv(f'{source}/{file}')
    data_to_extract = df.iloc[:, :4]
    data_to_extract.to_csv(f'{destination_dir}/csv_data.csv', index=False)
    return 'Data extrcated succsessfulle!!'



def extract_data_from_tsv(source: str, file: str, destination: str):
    df = pd.read_csv(f'{source}/{file}', sep='\t')
    data_to_extract = df.iloc[:, :3]
    data_to_extract.to_csv(f'{destination}/tsv_data.csv', index=False)
    return 'TSV success!'

#/workspaces/ETL_DataPipeline_Shell_Airflow/python_finalassignment/dags/python_etl/staging/tolldata.tgz

print(download_dataset(url, source_dir))
print(unzip_tolldata(source_dir, source_file))
print(extract_data_from_csv(source_dir, 'vehicle-data.csv', destination_dir))
print(extract_data_from_tsv(source_dir, 'tollplaza-data.tsv', destination_dir))