#!/usr/bin/python

from pyspark.sql.functions import col, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from gcs_utils import load_files_from_gcs, get_spark
    
from config import (
    GCS_BUCKET, 
    LOG_FILES, 
    LOG_METADATA_FILE_PATH,
    BQ_DATASET_NAME,
    BQ_LOG_REVIEWS_TABLE 
    )

def main():
    # Initialize Spark session
    spark = get_spark(name="Process User Logs")

    # Define the schema for the log_reviews file
    schema = StructType([
        StructField("id_review", IntegerType(), True),
        StructField("logs", StringType(), True)
    ])

    # Specify the read options
    read_options = {
        "schema": schema,
        "header": "true"
    }

    # Load log reviews file(s) from GCS
    dataframes = load_files_from_gcs(
            spark=spark,
            bucket_name=GCS_BUCKET,
            directory_path=LOG_FILES,
            metadata_file_path=LOG_METADATA_FILE_PATH,
            file_format="csv",
            read_options=read_options
        )

    # Parse the XML string in the 'log' column
    for log_df in dataframes:
        log_df_parsed = log_df.select(
            col("id_review").alias("log_id"),
            regexp_extract(col("log"), "<logDate>(.*?)</logDate>", 1).alias("log_date"),
            regexp_extract(col("log"), "<device>(.*?)</device>", 1).alias("device"),
            regexp_extract(col("log"), "<os>(.*?)</os>", 1).alias("os"),
            regexp_extract(col("log"), "<location>(.*?)</location>", 1).alias("location"),
            # Assuming browser information is present in the XML (not present in the log file)
            # regexp_extract(col("log"), "<browser>(.*?)</browser>", 1).alias("browser"),
            regexp_extract(col("log"), "<ipAddress>(.*?)</ipAddress>", 1).alias("ip"),
            regexp_extract(col("log"), "<phoneNumber>(.*?)</phoneNumber>", 1).alias("phone_number")
        )

    # Save the DataFrame to BigQuery
    (log_df_parsed.write.format("bigquery")
        .option("table", f"{BQ_DATASET_NAME}.{BQ_LOG_REVIEWS_TABLE}")
        .mode("append")
        .save()
        )

if __name__ == "__main__":
    main()
