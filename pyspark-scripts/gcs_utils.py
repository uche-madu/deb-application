# Import gcs using an alias to avoid conflict with sparknlp.common.storage
import google.cloud.storage as gcs_storage
from typing import List
from pyspark.sql import DataFrame, SparkSession
from config import SPARK_JARS


def check_file_exists(bucket_name: str, file_name: str) -> bool:
    """
    Check whether a file exists in a bucket.

    Args:
        bucket_name (str): Name of the bucket.
        file_name (str): Name of the 'file' to check.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    storage_client = gcs_storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    return blob.exists()

def initialize_metadata_file(bucket_name: str, metadata_file_path: str) -> None:
    """
    Initialize the metadata file in GCS if it doesn't exist.
    
    Args:
        bucket_name (str): Name of the GCS bucket.
        metadata_file_path (str): Path to the metadata file within the GCS bucket.

    Returns:
        None
    """
    storage_client = gcs_storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(metadata_file_path)
    
    if not blob.exists():
        blob.upload_from_string("")

def list_files_in_gcs(bucket_name: str, prefix: str) -> list:
    """
    List all files in a specified GCS bucket directory.

    Args:
        bucket_name (str): Name of the GCS bucket.
        prefix (str): Directory path in the GCS bucket to list files from.

    Returns:
        list: List of file paths in the specified GCS bucket directory.
    """
    storage_client = gcs_storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    
    return [blob.name for blob in blobs]

def get_processed_files(bucket_name: str, metadata_file_path: str) -> set:
    """
    Retrieve the list of processed files from the metadata file in GCS.

    Args:
        bucket_name (str): Name of the GCS bucket.
        metadata_file_path (str): Path to the metadata file within the GCS bucket.

    Returns:
        set: Set of processed file paths.
    """
    storage_client = gcs_storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(metadata_file_path)
    
    # Download the content of the metadata file
    content = blob.download_as_text()
    return set(content.splitlines())

def update_processed_files(bucket_name: str, metadata_file_path: str, processed_file: str) -> None:
    """
    Append a processed file to the metadata file in GCS.

    Args:
        bucket_name (str): Name of the GCS bucket.
        metadata_file_path (str): Path to the metadata file within the GCS bucket.
        processed_file (str): Path of the file that has been processed.

    Returns:
        None
    """
    processed_files = get_processed_files(bucket_name, metadata_file_path)
    processed_files.add(processed_file)
    
    storage_client = gcs_storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(metadata_file_path)
    
    # Update the metadata file with the new list of processed files
    blob.upload_from_string("\n".join(processed_files))

def load_files_from_gcs(
    spark: SparkSession,
    bucket_name: str,
    directory_path: str,
    metadata_file_path: str,
    file_format: str = "csv",
    read_options: dict = None
) -> List[DataFrame]:
    """
    Load new files from a GCS directory into Spark DataFrames.

    Args:
        spark (SparkSession): The Spark session.
        bucket_name (str): Name of the GCS bucket.
        directory_path (str): Directory path in the GCS bucket.
        metadata_file_path (str): Path to the metadata file in GCS.
        file_format (str, optional): The format of the files (e.g., "csv", "parquet"). Defaults to "csv".
        read_options (dict, optional): Additional options for reading files into Spark.

    Returns:
        List[DataFrame]: List of Spark DataFrames loaded from new files in GCS.
    """
    
    # Initialize the metadata file the first time
    initialize_metadata_file(bucket_name, metadata_file_path)

    # Get the list of all files currently in the directory
    all_files = list_files_in_gcs(bucket_name, directory_path)
    
    # Get the list of already processed files
    processed_files = get_processed_files(bucket_name, metadata_file_path)

    # Determine which files are new and need to be processed
    new_files = [file for file in all_files if file not in processed_files]

    dataframes = []
    for file in new_files:
        # Load data from GCS
        df = (spark.read.format(file_format)
              .options(**(read_options or {}))
              .load(f"gs://{bucket_name}/{file}"))
        
        dataframes.append(df)

        # Update the metadata file with the list of newly processed files
        update_processed_files(bucket_name, metadata_file_path, file)

    return dataframes

# If there are conflicts, the cluster-level settings defined in
# CLUSTER_GENERATOR_CONFIG in the Airflow DAG script will take precedence.
def get_spark(
    master: str = "yarn", 
    name: str = "Spark Application"
) -> SparkSession:
    """
    Initialize and return a Spark session with the specified configurations.
    
    Args:
        master (str, optional): 
            The master URL to connect to. For example:
            - "local" to run locally with one thread
            - "local[4]" to run locally with 4 cores
            - "local[*]" to run locally with as many worker threads as logical cores on the machine
            - "yarn" to run on a YARN cluster. 
            Defaults to "yarn".
            
        name (str, optional): 
            A name for the Spark application. 
            Defaults to "Spark Application".
        
    Returns:
        SparkSession: The created Spark session.
    """
    
    builder = SparkSession.builder.appName(name).master(master)
    builder.config('spark.jars.packages', ",".join(SPARK_JARS))
    builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    builder.config("spark.kryoserializer.buffer.max", "2000M")
    builder.config("spark.driver.maxResultSize", "0")
    return builder.getOrCreate()