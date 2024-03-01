This project is to create a Data Pipeline using Airflow under Docker Environment.

Run the following command to set up the Airflow:

Make sure you are in the directory containing the docker-compose.yaml file before running the command:
docker-compose up

In the airflow/dags folder, 

a. DAG to Fetch the data: fetch_data.py : A pipeline created to do the following tasks:
    1. Retrieve the links from a base url
    2. Download the csv files using python operator
    3. Zip the files into an archive and store in desired directory.

b. DAG to Process the data and produce plots: process_data.py : A pipeline created to do the following tasks:
    1.
    2.
    3.


