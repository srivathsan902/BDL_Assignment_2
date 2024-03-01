import os
import shutil
import random
import zipfile
import apache_beam
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
# def fileSensor():

def decide_branch(**context):
    file_found = context['ti'].xcom_pull(task_ids='waiting_for_file')
    if not file_found:
        return 'unzip_files'
    else:
        return 'stop_pipeline'


def unzip():
    zip_file_path = "/opt/airflow/logs/archive.zip"
    
    destination_dir = "/opt/airflow/logs/archive/"
    
    if os.path.exists(destination_dir):
        shutil.rmtree(destination_dir)
    os.makedirs(destination_dir)

    extract_to = destination_dir
    with zipfile.ZipFile(zip_file_path,"r") as zip_ref:
        zip_ref.extractall(destination_dir)
    
def delete():
    delete_dir = "/opt/airflow/logs/archive.zip"
    print('Removing Archive.zip')
    if os.path.exists(delete_dir):
        os.remove(delete_dir)
    
args={
    'owner' : 'Srivathsan',
    'retries': -1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id = "AnalyticsPipeline",
    default_args = args,
    schedule_interval = '*/1 * * * *',
    start_date = datetime(2024, 3, 1),
    catchup = False,
) as dag:

    # Define tasks
    sensing_task = FileSensor(task_id = 'waiting_for_file', filepath = '/opt/airflow/logs/archive.zip',soft_fail=True, poke_interval = 1, timeout = 5, fs_conn_id = 'local_files')

    branch_task = BranchPythonOperator(task_id = 'decide_branch', provide_context = True, python_callable = decide_branch)
    stop_pipeline_task = DummyOperator(task_id ='stop_pipeline')
    unzip_task = PythonOperator(task_id = 'unzip_files', python_callable = unzip, provide_context = True)
    delete_task = PythonOperator(task_id ='delete_files', python_callable = delete, provide_context = True)
    # Define dependencies
    sensing_task >> branch_task
    branch_task >> [unzip_task, stop_pipeline_task]

    unzip_task >> delete_task
    # file_not_found_branch >> stop_pipeline_task
