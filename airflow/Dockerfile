FROM apache/airflow:2.8.1
COPY requirements.txt .
RUN pip install -r requirements.txt

USER root
RUN sudo apt-get update && \
    apt-get install -y wget && \
    rm -rf /var/lib/apt/lists/*