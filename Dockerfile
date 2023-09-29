# Define the Airflow version
ARG AIRFLOW_VERSION=2.7.1

# Using the official Apache Airflow image
FROM apache/airflow:${AIRFLOW_VERSION}

WORKDIR /usr/local/airflow

COPY requirements.txt .

# install dbt into a virtual environment
# The echo line is to handle --user error
RUN python -m venv dbt_venv && \
    echo "include-system-site-packages = true" >> dbt_venv/pyvenv.cfg && \
    . dbt_venv/bin/activate && \
    pip install --upgrade pip && \
    pip install --no-cache-dir dbt-bigquery && \
    deactivate

RUN pip install --upgrade pip && \
    pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt