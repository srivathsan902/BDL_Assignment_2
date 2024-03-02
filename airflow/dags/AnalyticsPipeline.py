import os
import shutil
import random
import zipfile
import pandas as pd
import apache_beam as beam
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
# def fileSensor():


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


def extract_required_fields(file_path):
    df = pd.read_csv(file_path)
    columns = ['DATE', 'LATITUDE', 'LONGITUDE', 'HourlyWindSpeed', 'HourlyDryBulbTemperature']
    columns_to_convert = ['LATITUDE', 'LONGITUDE', 'HourlyWindSpeed', 'HourlyDryBulbTemperature']
    df = df[columns]
    df['DATE'] = pd.to_datetime(df['DATE'])

    df[columns_to_convert] = df[columns_to_convert].apply(pd.to_numeric, errors='coerce')
    df = df.dropna(how='any')

    return df

def compute_monthly_averages(df):
    df.set_index('DATE', inplace=True)
    monthly_avg_df = df.resample('M').mean().round(3)
    monthly_avg_df['month'] = monthly_avg_df.index.strftime('%B')
    monthly_avg_df.set_index('month', inplace=True)
    
    output_file = os.path.join('/opt/airflow/logs/processed_archive', "test.csv")
    monthly_avg_df.to_csv(output_file, index=False)

    LATITUDE = monthly_avg_df['LATITUDE'].mean().round(3)
    LONGITUDE = monthly_avg_df['LONGITUDE'].mean().round(3)

    monthly_avg_df = monthly_avg_df.drop(['LATITUDE','LONGITUDE'],axis = 1)

    monthly_fields = tuple(tuple(monthly_avg_df[col].to_numpy()) for col in monthly_avg_df.columns)

    monthly_fields = (LATITUDE, LONGITUDE) + monthly_fields
    print(monthly_fields)
    return monthly_fields


def run():
    input_folder = "/opt/airflow/logs/archive"  # Replace with the actual path to your CSV folder
    output_directory = "/opt/airflow/logs/processed_archive/result.txt"  # Replace with the desired output folder

    # Automatically detect CSV files in the specified folder
    csv_files = [os.path.join(input_folder, file) for file in os.listdir(input_folder) if file.endswith(".csv")]

    with beam.Pipeline(runner='DirectRunner') as pipeline:
        # Read CSV files in parallel
        csv_data = (
            pipeline
            | "Read CSV files" >> beam.Create(csv_files)
            | "Extract Required Fields" >> beam.Map(extract_required_fields)
            | "Compute Monthly Averages" >> beam.Map(compute_monthly_averages)
            | "Write to Output" >> beam.io.WriteToText(output_directory, file_name_suffix="", shard_name_template='', num_shards=1)
            
        )























args={
    'owner' : 'Srivathsan',
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id = "AnalyticsPipeline",
    default_args = args,
    schedule_interval = '*/5 * * * *',
    start_date = datetime(2024, 3, 1),
    catchup = False,
) as dag:

    # Define tasks
    sensing_task = FileSensor(task_id = 'waiting_for_file', filepath = '/opt/airflow/logs/archive.zip',soft_fail=True, poke_interval = 1, timeout = 5, fs_conn_id = 'local_files')

    unzip_task = PythonOperator(task_id = 'unzip_files', python_callable = unzip, provide_context = True)
    # delete_task = PythonOperator(task_id ='delete_files', python_callable = delete, provide_context = True)

    process_csv_task = PythonOperator(task_id ='process_csv', python_callable = run, provide_context = True)
    # Define dependencies
    sensing_task >> unzip_task >> process_csv_task
    