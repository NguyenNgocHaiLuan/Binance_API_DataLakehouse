FROM apache/airflow:2.7.1

USER root
RUN apt-get update && \
    apt-get install -y docker.io && \
    apt-get clean

COPY requirements.txt /tmp/requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements.txt