import requests
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


input_file = '../demo_files/web-server-access-log.txt'
extracted_file = '../demo_files/extracted-data.txt'
output_file = '../demo_files/capitalized.txt'


url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt'


def download_file():
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        #input_file.parent.mkdir(parents=True, exist_ok=True)
        with open(input_file, 'w') as f:
            f.write(response.text)
    except requests.exceptions.RequestException as e:
        logging.error(f"Download failed: {e}")
        
    else:
        print(f"File downloaded successfully: {input_file}")




def extract():
    with open(input_file, 'r') as infile, open(extracted_file, 'w') as outfile:
        # need timestamp and visitor_id
        for line in infile:
            fields = line.split('#')
            #print(fields[0], fields[3])
            timestamp=fields[0]
            visitorid=fields[3]
            outfile.write(f'{timestamp}, {visitorid}\n')
        logging.info(f"Data extracted and saved to: {extracted_file}")



def transform():
    with open(extracted_file, 'r', encoding='UTF-8') as infile, open(output_file, 'w') as outfile:
        print(f'READ {extracted_file}')
        content = infile.readlines()
        columns = 'timestamp, visitorid, year, time\n'
        outfile.write(f'{columns}')
        for el in content[1:]:
            fields = el.split(',')
            timestamp = fields[0].strip()
            visitorid = fields[1].strip()
            year = fields[0][:4].strip()
            time = fields[0].split()[1]
            outfile.write(f'{timestamp}, {visitorid}, {year}, {time}\n')
        logging.info(f"Transformation complete. Output saved to: {output_file}")
            


if __name__ == '__main__':
    download_file()
    extract()
    transform()
