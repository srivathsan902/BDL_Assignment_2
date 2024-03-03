import os
import re
import json
import shutil
import random
import zipfile
import imageio
import subprocess
import numpy as np
import pandas as pd
import geopandas as gpd
from matplotlib import cm
import apache_beam as beam
from airflow.models import DAG
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

# Define variables:
# Add columns here to analyze
COLUMNS_TO_ANALYZE = ['HourlyWindSpeed', 'HourlyDryBulbTemperature']


def unzip():
    zip_file_path = "/opt/airflow/logs/archive.zip"
    
    destination_dir = "/opt/airflow/logs/archive/"
    
    # Create directory if it doesn't exist
    if os.path.exists(destination_dir):
        shutil.rmtree(destination_dir)
    os.makedirs(destination_dir)

    extract_to = destination_dir
    with zipfile.ZipFile(zip_file_path,"r") as zip_ref:
        zip_ref.extractall(destination_dir)
    

def extract_required_fields(file_path):
    df = pd.read_csv(file_path)
    columns = ['DATE', 'LATITUDE', 'LONGITUDE'] + COLUMNS_TO_ANALYZE
    columns_to_convert = ['LATITUDE', 'LONGITUDE'] + COLUMNS_TO_ANALYZE
    df = df[columns]
    # df['DATE'] = pd.to_datetime(df['DATE'])
    # print(file_path)
    df['DATE'] = df['DATE'].apply(pd.to_datetime, errors='coerce')
    df = df.dropna(how='any')
    # df.dropna(subset=['DATE'], inplace=True)

    df[columns_to_convert] = df[columns_to_convert].apply(pd.to_numeric, errors='coerce')
    df = df.dropna(how='any')

    return df

def compute_monthly_averages(df):
    df.set_index('DATE', inplace=True)
    start_year = df.index.min().year    
    full_date_range = pd.date_range(start=f'{start_year}-01-01', end=f'{start_year}-12-31', freq='M')

    # Put average of 0 for the month, if the month is not encountered at all in the file
    monthly_avg_df = df.resample('M').mean().round(3).reindex(full_date_range).fillna(0)

    monthly_avg_df['month'] = monthly_avg_df.index.strftime('%B')
    monthly_avg_df.set_index('month', inplace=True)
    

    LATITUDE = monthly_avg_df['LATITUDE'].mean().round(3)
    LONGITUDE = monthly_avg_df['LONGITUDE'].mean().round(3)

    monthly_avg_df = monthly_avg_df.drop(['LATITUDE','LONGITUDE'],axis = 1)

    monthly_avg_fields = tuple(tuple(monthly_avg_df[col].to_numpy()) for col in monthly_avg_df.columns)

    monthly_avg_fields = (LATITUDE, LONGITUDE) + monthly_avg_fields
    # Json dump to ensure while reading back,it comes as a list and not string
    monthly_avg_fields = json.dumps(monthly_avg_fields)
    return monthly_avg_fields


def run():      # Contains the Apache Beam Framework
    input_folder = "/opt/airflow/logs/archive" 

    # Unique identifier name is the Year considered
    unique_identifier_name = [os.path.splitext(file)[0] for file in os.listdir(input_folder) if file.endswith(".txt")][0]

    destination_folder = '/opt/airflow/logs/final_data_archive'

    if os.path.exists(destination_folder):
        shutil.rmtree(destination_folder)
    os.makedirs(destination_folder)

    output_directory = os.path.join(destination_folder, unique_identifier_name + ".txt")

    # Automatically detect CSV files in the specified folder
    csv_files = [os.path.join(input_folder, file) for file in os.listdir(input_folder) if file.endswith(".csv")]

    with beam.Pipeline(runner='DirectRunner') as pipeline:
        # Process CSV files in parallel
        csv_data = (
            pipeline
            | "Read CSV files" >> beam.Create(csv_files)
            | "Extract Required Fields" >> beam.Map(extract_required_fields)
            | "Compute Monthly Averages" >> beam.Map(compute_monthly_averages)
            | "Write to Output" >> beam.io.WriteToText(output_directory, file_name_suffix="", shard_name_template='', num_shards=1)

        )


def generate_plot(DATA, FIELD_NAME, month, year):
    # inverse_month is to get the title of the plot appropriately
    inverse_month = {"a": "January", "b": "February", "c": "March", 'd': "April", 'e': "May", 'f': "June", 'g': "July", 'h': "August", 'i': "September", 'j': "October", 'k': "November", 'l': "December"}
    
    columns = ['LATITUDE', 'LONGITUDE'] + [FIELD_NAME]
    gdf = gpd.GeoDataFrame(DATA, columns=columns, geometry=gpd.points_from_xy(DATA[:,1], DATA[:,0]))
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres')) # to overlay on the world map
    fig, ax = plt.subplots(figsize=(10, 10))
    world.plot(ax=ax, color='lightgray')

    # Normalize to give input to the color map
    norm = plt.Normalize(gdf[FIELD_NAME].mean() - gdf[FIELD_NAME].var()**0.5,
                     gdf[FIELD_NAME].mean() + gdf[FIELD_NAME].var()**0.5)

    gdf.plot(
        ax=ax, 
        marker='o', 
        color=cm.coolwarm(norm(gdf[FIELD_NAME])),  # Normalize rainfall for colormap
        markersize=100,  # Fix marker size
        alpha=0.7, 
        legend=True, 
        legend_kwds={'label': FIELD_NAME}
    )
    
    # Customize for getting the colorbar of the heat map
    cax = fig.add_axes([0.92, 0.1, 0.02, 0.8])  # [x, y, width, height]
    sm = plt.cm.ScalarMappable(cmap=cm.coolwarm, norm=norm)
    sm.set_array([])  # You need to set a dummy array for the colorbar to work
    fig.colorbar(sm, cax=cax, label=FIELD_NAME)
    ax.set_title('Average '+FIELD_NAME + "  " + inverse_month[month] + " :" + year)


def generate_png():
    input_folder = "/opt/airflow/logs/final_data_archive/"
    output_folder = "/opt/airflow/logs/png_archive/"

    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)
    os.makedirs(output_folder)

    # unique_identifier_name is the year considered
    unique_identifier_name = [os.path.splitext(file)[0] for file in os.listdir(input_folder) if file.endswith(".txt")][0]
    input_file_path = os.path.join(input_folder, unique_identifier_name+'.txt')

    # Global data stores the fields over all locations
    global_data = []

    with open(input_file_path, 'r') as file:
        for line in file:
            # Use json.loads to convert each line to a tuple
            loacl_data = json.loads(line)
            # local data is for a specific latitude and longitude
            global_data.append(loacl_data)

    # to create the png with ascending order of month name, assign alphabets in sequence as proxy for the month
    month = {1:"a",2:"b",3:"c",4:"d",5:"e",6:"f",7:"g",8:"h",9:"i",10:"j",11:"k",12:"l"}

    for i in range(len(COLUMNS_TO_ANALYZE)):  # For iterating over every climatic field
        for j in range(12): # For iterating over every month
            field_data = []
            for k in range(len(global_data)):   # For iterating over every location
                field_data.append([global_data[k][0],global_data[k][1],global_data[k][i+2][j]])

            field_data = np.array(field_data)
            plot_name = unique_identifier_name + "_" + month[j+1] + "_" + COLUMNS_TO_ANALYZE[i]
            generate_plot(field_data, COLUMNS_TO_ANALYZE[i], month[j+1], unique_identifier_name)

            plot_name = unique_identifier_name + "_" + month[j+1] + "_" + COLUMNS_TO_ANALYZE[i]
            plt.savefig(output_folder + plot_name + ".png", format="png")
    
    return True

def create_gif():
    # Create gifs using the png files generated before
    fps=12
    input_folder = "/opt/airflow/logs/png_archive/"
    output_folder = "/opt/airflow/logs/gif_archive/"

    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)
    os.makedirs(output_folder)

    unique_identifier_name = [os.path.splitext(file)[0].split('_')[0] for file in os.listdir(input_folder) if file.endswith(".png")][0]

    for i in range(len(COLUMNS_TO_ANALYZE)):
        FIELD_NAME = COLUMNS_TO_ANALYZE[i]

        output_filename = unique_identifier_name + "_" + FIELD_NAME + ".gif"
        output_gif_path = os.path.join(output_folder, output_filename)

        # Extract all the png files having the same field name
        png_files = sorted([f for f in os.listdir(input_folder) if f.lower().endswith(FIELD_NAME.lower()+'.png')])
        
        input_files = [os.path.join(input_folder, file) for file in png_files]

        # Read PNG files and create GIF
        images = [imageio.imread(file) for file in input_files]
        imageio.mimsave(output_gif_path, images, duration=1000/fps, loop=3)

    return True

def remove_unnecessary_files():

    folder = "/opt/airflow/logs/png_archive/"
    for file in os.listdir(folder):
        if file.lower().endswith('.png'):
            file_path = os.path.join(folder, file)
            os.remove(file_path)
            
    folder = "/opt/airflow/logs/png_archive/"
    if os.path.exists(folder):
        shutil.rmtree(folder)

    folder = "/opt/airflow/logs/archive/"
    if os.path.exists(folder):
        shutil.rmtree(folder)

    folder = "/opt/airflow/logs/archive.zip"
    if os.path.exists(folder):
        os.remove(folder)

    folder = "/opt/airflow/logs/links.txt"
    if os.path.exists(folder):
        os.remove(folder)
    
    folder = "/opt/airflow/logs/final_data_archive.txt"
    if os.path.exists(folder):
        os.remove(folder)

    return True


# ********************************************************************************************************************************
# ********************************************************************************************************************************
# ********************************************************************************************************************************

# DAG Definition
args={
    'owner' : 'Srivathsan',
    'retries': 0,
}

with DAG(
    dag_id = "AnalyticsPipeline",
    default_args = args,
    schedule_interval = timedelta(minutes =1),
    start_date = datetime(2024, 3, 1),
    catchup = False,
) as dag:

    # Define tasks
    sensing_task = FileSensor(task_id = 'waiting_for_file', filepath = '/opt/airflow/logs/archive.zip',soft_fail=True, poke_interval = 1, timeout = 5, fs_conn_id = 'local_files')

    unzip_task = PythonOperator(task_id = 'unzip_files', python_callable = unzip, provide_context = True)

    process_csv_task = PythonOperator(task_id ='compute_monthly_averages', python_callable = run, provide_context = True)

    generate_png_task = PythonOperator(task_id ='generate_png', python_callable = generate_png, provide_context = True)

    create_gif_task = PythonOperator(task_id ='create_gif', python_callable= create_gif, provide_context = True)

    remove_unnecessary_files_task = PythonOperator(task_id ='remove_unnecessary_files', python_callable= remove_unnecessary_files, provide_context = True)
    
    # Define dependencies
    sensing_task >> unzip_task >> process_csv_task >> generate_png_task >> create_gif_task >> remove_unnecessary_files_task
    