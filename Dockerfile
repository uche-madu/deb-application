# Define the Airflow version  
ARG AIRFLOW_VERSION=2.7.3 

# Using the official Apache Airflow image 
FROM apache/airflow:${AIRFLOW_VERSION}  

ARG AIRFLOW_VERSION

WORKDIR /usr/local/airflow

USER root 

RUN mkdir /root/.dbt
COPY profiles.yml /root/.dbt

RUN mkdir -p dags/dbt/deb-capstone
COPY dbt/deb-capstone/dbt_project.yml dags/dbt/deb-capstone
COPY dbt/deb-capstone/ dags/dbt/deb-capstone

USER airflow
# Install dbt into a virtual environment
# The sed command allows pip to avoid running with --user flag in the virtual environment
RUN python -m venv dbt_venv && \
    sed -i 's/include-system-site-packages = false/include-system-site-packages = true/' dbt_venv/pyvenv.cfg && \
    . dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery && \
    deactivate

COPY requirements.txt . 

RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt
