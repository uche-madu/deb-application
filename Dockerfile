# Using the official Apache Airflow image
FROM apache/airflow:2.7.1

WORKDIR /usr/local/airflow

COPY requirements.txt .

# install dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery && deactivate

RUN pip install --no-cache-dir -r requirements.txt