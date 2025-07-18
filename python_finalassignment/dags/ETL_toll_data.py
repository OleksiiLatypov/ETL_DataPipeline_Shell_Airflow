import pandas as pd 
import tarfile
import os
import requests
import logging 


logging.basicConfig(level=logging.INFO,  format='%(asctime)s - %(levelname)s - %(message)s')


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
    
    logging.info(f"Downloaded dataset to {file_path}")



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
            logging.info('Unzip Successfully!')
    except Exception as e:
        logging.ERROR(f"Error extracting {source}: {e}")
    


def extract_data_from_csv(source: str, file: str, destination_dir: str):
    df = pd.read_csv(f'{source}/{file}', sep=',', header=None)
    data_to_extract = df.iloc[:, :4]
    data_to_extract.to_csv(f'{destination_dir}/csv_data.csv', index=False, header=['Rowid', 'Timestamp', 'Anonymized_Vehicle_number', 'Vehicle_type'])
    logging.info('Data extrcated succsessfulle!!')



def extract_data_from_tsv(source: str, file: str, destination: str):
    df = pd.read_csv(f'{source}/{file}', sep='\t', header=None)
    data_to_extract = df.iloc[:, 4:]
    data_to_extract.to_csv(f'{destination}/tsv_data.csv', index=False, header=['Number_of_axles', 'Tollplaza_id', 'Tollplaza code'])
    logging.info('TSV success!')



def extract_data_from_fixed_width(source: str, file: str, destination: str):
    with open(f'{source}/{file}', 'r') as f_in, open (f'{destination}/fixed_width_data.csv', 'w') as f_out:
        header= f'Type_of_payment_code,Vehicle_Code\n'
        f_out.write(header)
        for line in f_in:
            payment_code = line.split()[-2].strip()
            vehicle_code = line.split()[-1].strip()
            f_out.write(f'{payment_code},{vehicle_code}\n')
        logging.info('extract_data_from_fixed_width')



def consolidate_data(destination: str, *args):
    csv_data, tsv_data, fixed_width_data, extracted_data = args
    df_1 = pd.read_csv(f'{destination}/{csv_data}')
    df_2 = pd.read_csv(f'{destination}/{tsv_data}')
    df_3 = pd.read_csv(f'{destination}/{fixed_width_data}')
    final_df = pd.concat([df_1, df_2, df_3], axis=1)
    final_df.columns = final_df.columns.str.upper()
    final_df.to_csv(f'{destination}/{extracted_data}', index=False)
    logging.info('Consolidate data saved successefully!')
        


def transform_data(destination: str):
    df = pd.read_csv(f'{destination}/extracted_data.csv')
    df['VEHICLE_TYPE'] = df['VEHICLE_TYPE'].str.upper()
    df.to_csv(f'{destination}/transformed.csv')
    logging.info('capitilized letters transformed')

print(download_dataset(url, source_dir))
print(unzip_tolldata(source_dir, source_file))
print(extract_data_from_csv(source_dir, 'vehicle-data.csv', destination_dir))
print(extract_data_from_tsv(source_dir, 'tollplaza-data.tsv', destination_dir))
print(extract_data_from_fixed_width(source_dir, 'payment-data.txt', destination_dir))
print(consolidate_data(destination_dir, 'csv_data.csv', 'tsv_data.csv', 'fixed_width_data.csv', 'extracted_data.csv'))
print(transform_data(destination_dir))