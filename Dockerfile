FROM apache/airflow:2.4.3
USER root

COPY requirements.txt /
RUN sudo pip install -r /requirements.txt

USER airflow
    
