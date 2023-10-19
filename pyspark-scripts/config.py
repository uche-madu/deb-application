import os

GCS_BUCKET = "deb-capstone"

# Data directories
MOVIE_FILES = os.path.join(GCS_BUCKET, "project-data", "movie_reviews")
LOG_FILES = os.path.join(GCS_BUCKET, "project-data", "log_reviews")

# Metadata directories
METADATA_DIR = os.path.join(GCS_BUCKET, "project-data", "metadata")
MOVIES_METADATA_FILE_PATH = os.path.join(METADATA_DIR, "movie_reviews_metadata.txt")
LOG_METADATA_FILE_PATH = os.path.join(METADATA_DIR, "log_reviews_metadata.txt")

# Model directory
MODEL_DIR = os.path.join(GCS_BUCKET, "models", "sentiment_spark_nlp")

# My HuggingFace sentiment model fine-tuned using IMDb movie reviews dataset.
MODEL_NAME = "dreemer6/bert-finetuned-sst2"

SPARK_JARS = ["com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2",
              "com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.2"]

# Define BigQuery dataset and table
BQ_DATASET_NAME = "movie_analytics"
BQ_MOVIE_REVIEWS_TABLE = "classified_movie_review" 
BQ_LOG_REVIEWS_TABLE = "review_logs"