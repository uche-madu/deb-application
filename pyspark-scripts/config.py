GCS_BUCKET = "deb-capstone"
MOVIE_FILES = "{}/project-data/movie_reviews".format(GCS_BUCKET)
MOVIES_METADATA_FILE_PATH = "{}/processed_files_metadata.txt".format(MOVIE_FILES)
MODEL_DIR = "{}/models/sentiment_spark_nlp".format(GCS_BUCKET)

# My HuggingFace sentiment model fine-tuned using IMDb movie reviews dataset.
MODEL_NAME = "dreemer6/bert-finetuned-sst2"

SPARK_JARS = ["com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2",
              "com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.2"]

# Define BigQuery dataset and table
BQ_DATASET_NAME = "movie_analytics"
BQ_MOVIE_REVIEWS_TABLE = "classified_movie_review" 
BQ_LOG_REVIEWS_TABLE = "review_logs"

LOG_FILES = "{}/project-data/log_reviews".format(GCS_BUCKET)
LOG_METADATA_FILE_PATH = "{}/processed_files_metadata.txt".format(LOG_FILES)
