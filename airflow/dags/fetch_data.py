import os
import shutil
import random
import zipfile
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator


# Define variables
BASE_URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/"
YEAR = 1973
BASE_URL = BASE_URL + str(YEAR) +'/'
DESTINATION_FILE_PATH = '/opt/airflow/logs/links.txt'
SAMPLES_TO_CHOOSE = 2


bash_command = f"""
wget -q -O - {BASE_URL} |
grep -oP 'href="([^"]*\\.csv)"' |
sed -e 's/^href="//' -e 's/"$//' -e "s|^|{BASE_URL}|" > {DESTINATION_FILE_PATH}
"""

# Import custom modules:
def select_urls(**context):
    path = '/opt/airflow/logs/links.txt'
    all_links = None
    try:
        with open(path, 'r') as file:
            all_links = file.read()
    except OSError:
        print("Could not open/read file:")

    all_links = all_links.split('\n')
    required_links = random.sample(all_links, min(SAMPLES_TO_CHOOSE, len(all_links)))

    context['ti'].xcom_push(key = 'download_links', value = required_links)

    return True

def download_urls(**context):
    destination_dir = "/opt/airflow/logs/dataset_downloads/"
    
    if os.path.exists(destination_dir):
        shutil.rmtree(destination_dir)
    os.makedirs(destination_dir)

    links = context['ti'].xcom_pull(key='download_links')

    for link in links:
        print(link)
        os.system(f"wget -P {destination_dir} {link}")

def zip_files(**context):
    download_dir = "/opt/airflow/logs/dataset_downloads"
    destination_dir = "/opt/airflow/logs/archive.zip"
    zip_file_path = "/opt/airflow/logs/"
    
    if os.path.exists(destination_dir):
        os.remove(destination_dir)

    archive_name = 'archive'
    shutil.make_archive(archive_name, 'zip', download_dir)
    shutil.move(f"{archive_name}.zip", os.path.join(zip_file_path, f"{archive_name}.zip"))
    shutil.rmtree(download_dir)

# Define DAG
with DAG(
    dag_id = "Fetch_Data_DAG",
    schedule_interval = '@daily',
    start_date = datetime(2024, 3, 1),
    catchup = False
) as dag:

    # Define tasks
    fetch_url_task = BashOperator(task_id = 'fetch_links', bash_command = bash_command)
    select_urls_task = PythonOperator(task_id = 'select_urls', python_callable = select_urls, provide_context = True)
    download_urls_task = PythonOperator(task_id = 'download_urls', python_callable = download_urls, provide_context = True)
    zip_files_task = PythonOperator(task_id = 'zip_files', python_callable = zip_files, provide_context = True)

    # Define dependencies
    fetch_url_task >> select_urls_task >> download_urls_task >> zip_files_task
