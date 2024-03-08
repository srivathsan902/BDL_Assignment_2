# Data Pipeline

## Overview
The National Centers for Environmental Information contains several data pertaining to climatological parameters observed over diverse set of locations all around the world. The data is collected over 13400 stations, and it is collected every hour. It is of engineering interest to analyse these data, and gain insights about the climate. This project is to create a pipeline for the flow of this vast sets of data, and create simple plots to visualize the monthly variation of the parameters through an year.
The project utilizes Apache airflow to create the pipeline and docker is used to host the setup.

## Installation
Install Docker and WSL to your local computer.

Create a venv in the Pipeline directory using the command: `python -m venv venv` and the activate it using `venv\Scripts\activate`. After this install the packages in the requirements.txt file using the command: `pip install -r requirements.txt`.

Run the following command to set up the Airflow:

Open the terminal with Ubuntu and make sure you are in the directory containing the **docker-compose.yaml** file (in this case,airflow) before running the command: `docker-compose up build` . For subsequent usages, `docker-compose up` suffices. THis will trigger the initialization of airflow, and once it is up, you can log in to the local host at : `localhost:8080`. 
Default login ID and password are both set to **airflow**.

## Walkthrough of Code
In the `airflow/dags` folder, all the DAGs(Directed Acyclic Graphs) are created. These can be triggered/ scheduled and the progress can be seen in the local host. Each DAG involves a series of operations. This project involves 2 such DAGs, one for fetching the data and another for analysing the fetched data.

* DAG to Fetch the data: `DataFetchPipeline.py` : A pipeline created to do the following tasks:
    1. Retrieve the links from a base url.
    2. Select desired number of files to download and download the csv files from the webpage.
    3. Zip the files into an archive and store in desired directory.

* DAG to Process the data and produce plots: `AnalyticsPipeline.py` : A pipeline created to do the following tasks:
    1. File sensor to sense if the archive of files is present.
    2. Unzip the files and analyse each file in parallel using Apache Beam Pipeline.
    3. Create plots and then a gif animation for each parameter that is under analysis.

## Additional Information
In case you add some code on top of this, you may have to add the libraries that you use in the `requirements.txt` file in the `airflow` as well as `Pipeline` directory. Repeat the `pip install -r requirements.txt` command and `docker-compose up build` command as done earlier.

Note that all the files are written into the `airflow/logs` folder only. This is the only location airflow can take data from and store into the system. So navigate for any created files from this folder. Ensure to delete any created folder after use, as it can accumulate quickly is left unnoticed.

Once the usage is over, ensure to run the command `docker-compose down` to shutdown the local host.
In case your system consumes a lot of memory and disk usage, then open the command promt with administrator access and run `wsl --shutdown` after this.

## Helpful Links
* [Youtube Resource for setting up docker](https://www.youtube.com/watch?v=4gz9SogFh1Q)
* [Youtube Resource for airflow tutorial](https://www.youtube.com/watch?v=20HDFbYyAY0)
* [Docker Desktop Windows](https://docs.docker.com/desktop/install/windows-install/)
* [docker-compose.yaml](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)



