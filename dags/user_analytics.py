import pendulum
import requests
import csv
import logging
import pandas as pd
from datetime import datetime

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocSubmitPySparkJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

GCP_CONN_ID='GCP'
POSTGRES_CONN_ID='POSTGRES'
BUCKET_NAME='deb-capstone'
BQ_DATASET='movie_analytics'
USER_PURCHASE_TABLE='user_purchase_stg'
CLUSTER_NAME = 'deb-capstone'
PROJECT_ID = 'wizeline-deb'
REGION = 'us-central1'
ZONE = 'us-central1-c'

PYSPARK_JOB_PATH = f"gs://{BUCKET_NAME}/pyspark-scripts/"
PIP_INIT_FILE= f"gs://{BUCKET_NAME}/dataproc-initialization-actions/python/v1.0/pip-install.sh"
# CONNECTOR_INIT_FILE='gs://${BUCKET_NAME}/dataproc-initialization-actions/connectors/v1.0/connectors.sh'

CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    zone=ZONE,
    master_machine_type="n1-standard-4",
    master_disk_size=16,
    worker_machine_type="n1-standard-2",
    worker_disk_size=16,
    num_workers=2,
    storage_bucket=BUCKET_NAME,
    init_actions_uris=[PIP_INIT_FILE],
    metadata={"PIP_PACKAGES": "spark-nlp==5.1.2 google-cloud-storage==1.6.0 transformers==4.25.1 tensorflow==2.11.0"},
    properties={
        'spark:spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        'spark:spark.driver.maxResultSize': '0',
        'spark:spark.kryoserializer.buffer.max': '2000M',
        'spark:spark.jars.packages': 'com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.2',
        'spark:spark.jars': 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.2.jar'
    },
    num_preemptible_workers=1,
    preemptibility="PREEMPTIBLE",
    gcp_conn_id=GCP_CONN_ID,
).make()

create_schema_and_table_query="""
            CREATE SCHEMA IF NOT EXISTS user_analytics;
            CREATE TABLE IF NOT EXISTS user_analytics.user_purchase (
                invoice_number varchar(10),
                stock_code varchar(20),
                detail varchar(1000),
                quantity int,
                invoice_date timestamp,
                unit_price numeric(8,3),
                customer_id int,
                country varchar(20),
                PRIMARY KEY (invoice_number, stock_code)
            );
            """

insert_user_purchase_query = """
            INSERT INTO user_analytics.user_purchase (
                invoice_number, 
                stock_code, 
                detail, 
                quantity, 
                invoice_date, 
                unit_price,
                customer_id,
                country
                )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (invoice_number, stock_code) DO NOTHING;
            """

extract_user_purchase_query = """
            SELECT * FROM <schema>.user_purchase;
            """

@dag(
    schedule='@daily', 
    start_date=pendulum.datetime(2023, 10, 2, tz="UTC"), 
    catchup=False
)
def movie_analytics_dag() -> None:
    """
    Defines the movie analytics DAG which processes user purchase data and performs tasks on Dataproc.
    """
   
    @task_group(group_id="user_purchase_raw_to_stg")
    def user_purchase_raw_to_stg() -> None:
        """
        Task group that handles the extraction, transformation, and loading of user purchase data.
        """

        create_schema_and_table = PostgresOperator(
            task_id='create_schema_and_table',
            sql=create_schema_and_table_query,
            postgres_conn_id=POSTGRES_CONN_ID
        )

        @task
        def load_user_purchase_to_postgres() -> None:
            """
            Load user purchase data from GCS to Postgres.
            The data is read in chunks, cleaned, transformed, and then batch inserted into Postgres.
            """
            try:
                gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
                file_handle = gcs_hook.download(
                    bucket_name=BUCKET_NAME, 
                    object_name='project-data/user_purchases/user_purchase.csv', 
                    filename=None
                )
                
                # Read CSV data into a pandas DataFrame in chunks in case of large files
                chunks = pd.read_csv(file_handle, chunksize=50_000)

                for df in chunks:

                    # Count rows with missing 'customer_id'
                    missing_customer_id_count = df['CustomerID'].isna().sum()
                    
                    # Drop rows where 'customer_id' is missing
                    df.dropna(subset=['CustomerID'], inplace=True)
                    
                    # Log the number of rows dropped
                    logging.info(f"Dropped {missing_customer_id_count} rows with missing 'customer_id' values.")
                    
                    # Fill missing values for all columns with empty strings
                    filled_count = df.isna().sum().sum()  # Get total number of missing values before filling
                    df.fillna("", inplace=True)
                    
                    # Log the number of missing values filled
                    logging.info(f"Filled {filled_count} missing values with empty strings.")

                    # Rename columns
                    df.rename(columns={
                        'InvoiceNo': 'invoice_number',
                        'StockCode': 'stock_code',
                        'Description': 'detail',
                        'Quantity': 'quantity',
                        'InvoiceDate': 'invoice_date',
                        'UnitPrice': 'unit_price',
                        'CustomerID': 'customer_id',
                        'Country': 'country'
                    }, inplace=True)
                                    
                    # Convert data types
                    df['invoice_number'] = df['invoice_number'].astype(str)
                    df['stock_code'] = df['stock_code'].astype(str)
                    df['detail'] = df['detail'].astype(str)
                    df['quantity'] = df['quantity'].astype(int)
                    df['invoice_date'] = pd.to_datetime(df['invoice_date'], format='%m/%d/%Y %H:%M', errors='coerce')
                    df['unit_price'] = df['unit_price'].astype(float)
                    df['customer_id'] = df['CustomerID'].astype(int)
                    df['country'] = df['country'].astype(str)
                    
                    # Convert DataFrame to list of tuples
                    tuples = [tuple(x) for x in df.to_numpy()]

                    # Use PostgresHook's insert_rows method for batch inserts
                    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
                    pg_hook.insert_rows(
                        table="user_analytics.user_purchase", 
                        rows=tuples, 
                        target_fields=df.columns.tolist(),
                    )

            except Exception as e:
                # Log the exception for debugging purposes
                logging.error(f"Error while loading user purchase data to Postgres: {e}")
                # Re-raise the exception to stop the task and mark it as failed
                raise

        get_user_purchase_data = PostgresToGCSOperator(
            task_id="move_user_purchase_table_to_gcs",
            postgres_conn_id=POSTGRES_CONN_ID,
            gcp_conn_id=GCP_CONN_ID,
            sql=extract_user_purchase_query,
            bucket=BUCKET_NAME,
            filename="project_data/user_purchase/processed_user_purchase",
            export_format="parquet",
            gzip=False,
        )

        load_gcs_to_stg = GCSToBigQueryOperator(
            task_id='load_user_purchase_from_gcs_to_stg',
            bucket=BUCKET_NAME,
            source_objects=['project_data/user_purchase/processed_user_purchase.parquet'],
            source_format='PARQUET',
            skip_leading_rows=1,  # ignore the header row
            destination_project_dataset_table=f'{BQ_DATASET}.{USER_PURCHASE_TABLE}',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
        )
        
        # task dependencies
        load_to_postgres = load_user_purchase_to_postgres()

        create_schema_and_table >> load_to_postgres
        load_to_postgres >> get_user_purchase_data
        get_user_purchase_data >> load_gcs_to_stg
        
    user_purchase_raw_to_stg()

    @task_group(group_id="dataproc_tasks")
    def dataproc_tasks() -> None:
        """
        Task group that handles the creation of a Dataproc cluster, processing of movie reviews and log reviews,
        and deletion of the Dataproc cluster.
        """

        create_dataproc_cluster = DataprocCreateClusterOperator(
            task_id="create_dataproc_cluster",
            cluster_name=CLUSTER_NAME,
            region=REGION,
            cluster_config=CLUSTER_GENERATOR_CONFIG,
        )

        process_movie_reviews = DataprocSubmitPySparkJobOperator(
            task_id="process_movie_reviews",
            main=PYSPARK_JOB_PATH + "process_movies.py",
            cluster_name=CLUSTER_NAME,
            region=REGION,
            pyfiles=[
                PYSPARK_JOB_PATH + "config.py",
                PYSPARK_JOB_PATH + "gcs_utils.py"
            ],
        )

        process_log_reviews = DataprocSubmitPySparkJobOperator(
            task_id="process_log_reviews",
            cluster_name=CLUSTER_NAME,
            region=REGION,
            main=PYSPARK_JOB_PATH + "process_logs.py",
            pyfiles=[
                PYSPARK_JOB_PATH + "config.py",
                PYSPARK_JOB_PATH + "gcs_utils.py"
            ],
        )

        delete_dataproc_cluster = DataprocDeleteClusterOperator(
            task_id="delete_dataproc_cluster",
            cluster_name=CLUSTER_NAME,
            region=REGION,
        )

        # Define task dependencies
        create_dataproc_cluster >> [process_movie_reviews, process_log_reviews] >> delete_dataproc_cluster.as_teardown(setups=create_dataproc_cluster)
    
    dataproc_tasks()

movie_analytics_dag()
