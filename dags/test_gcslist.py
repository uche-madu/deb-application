from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator

@dag(schedule=timedelta(days=1), start_date=datetime(2023, 10, 19), catchup=False, default_args={
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
})
def gcs_list_buckets():

    GCSListObjectsOperator(
        task_id='list_buckets',
        bucket='deb-capstone',
        match_glob='**/*/.csv',
        gcp_conn_id='gcp' 
    )    

gcs_list_buckets_dag = gcs_list_buckets()
