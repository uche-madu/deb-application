# Import gcs using an alias to avoid conflict with sparknlp.common.storage
import io
import pandas as pd
import google.cloud.storage as gcs_storage
from typing import List, Tuple
from pyspark.sql import DataFrame, SparkSession
from config import SPARK_JARS

storage_client = gcs_storage.Client()

def check_file_exists(bucket_name: str, file_name: str) -> bool:
    """
    Check whether a file exists in a bucket.

    Args:
        bucket_name (str): Name of the bucket.
        file_name (str): Name of the 'file' to check.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    exists = blob.exists()
    if exists:
        print(f"File '{file_name}' exists in bucket '{bucket_name}'.")
    else:
        print(f"File '{file_name}' does not exist in bucket '{bucket_name}'.")
    return exists

def initialize_metadata_file(bucket_name: str, metadata_file_path: str, initial_content: str = "") -> None:
    """
    Initialize the metadata file in GCS if it doesn't exist.
    
    Args:
        bucket_name (str): Name of the GCS bucket.
        metadata_file_path (str): Path to the metadata file within the GCS bucket.

    Returns:
        None
    """
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(metadata_file_path)
    
    if not blob.exists():
        blob.upload_from_string(initial_content)
        print(f"Initialized metadata file '{metadata_file_path}' with initial content.")
    else:
        print(f"Metadata file '{metadata_file_path}' already exists.")

def list_files_in_gcs(bucket_name: str, prefix: str) -> list:
    """
    List all files in a specified GCS bucket directory.

    Args:
        bucket_name (str): Name of the GCS bucket.
        prefix (str): Directory path in the GCS bucket to list files from.

    Returns:
        list: List of file paths in the specified GCS bucket directory.
    """
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
    read_options: dict = None,
    chunk_size: int = 1_000_000
) -> Tuple[List[DataFrame], List[str]]:
    """
    Load new files from a GCS directory in chunks into Spark DataFrames.
    
    Args:
        spark (SparkSession): The Spark session.
        bucket_name (str): Name of the GCS bucket.
        directory_path (str): Directory path in the GCS bucket.
        metadata_file_path (str): Path to the metadata file in GCS.
        file_format (str, optional): Format of the files (e.g., "csv", "parquet"). Defaults to "csv".
        read_options (dict, optional): Additional options for reading files into Spark.
        chunk_size (int): Number of rows per chunk to load from each file. Defaults to 1,000,000.
        
    Returns:
        Tuple[List[DataFrame], List[str]]: List of Spark DataFrames for new files and list of new file names.
    """
    # Initialize the metadata file
    initialize_metadata_file(bucket_name, metadata_file_path)

    # Get all files in the directory
    all_files = list_files_in_gcs(bucket_name, directory_path)
    processed_files = get_processed_files(bucket_name, metadata_file_path)

    # Filter for unprocessed files
    new_files = [file for file in all_files if file not in processed_files]
    dataframes = []

    for file_name in new_files:
        print(f"Loading file in chunks: {file_name}")
        
        # Load the file from GCS in chunks to optimize memory usage
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        file_bytes = blob.download_as_bytes()
        file_handle = io.BytesIO(file_bytes)
        
        # Read the file in chunks and convert each to a Spark DataFrame
        for chunk in pd.read_csv(file_handle, chunksize=chunk_size, **(read_options or {})):
            spark_df = spark.createDataFrame(chunk)
            dataframes.append(spark_df)

        # Update metadata to mark the file as processed
        update_processed_files(bucket_name, metadata_file_path, file_name)

    return dataframes, new_files

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