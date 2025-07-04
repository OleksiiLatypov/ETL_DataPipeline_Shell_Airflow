import requests
import logging





input_file = 'web-server-access-log.txt'
url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt'


def download_file():
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        with open(input_file, 'w') as f:
            f.write(response.text)
    except requests.exceptions.RequestException as e:
        logging.error(f"Download failed: {e}")
        
    else:
        print(f"File downloaded successfully: {input_file}")

download_file()