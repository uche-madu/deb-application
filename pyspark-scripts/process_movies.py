#!/usr/bin/python

import tempfile
import os
import shutil

from transformers import TFBertForSequenceClassification, BertTokenizer 
import tensorflow as tf

# import sparknlp
from pyspark.ml import Pipeline
from sparknlp.annotator import *
from sparknlp.base import *
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from gcs_utils import (
    load_files_from_gcs, 
    get_spark,
    update_processed_files, 
    check_file_exists
    )

# Import configuration variables
from config import (
    GCS_BUCKET, 
    MOVIE_FILES, 
    MOVIES_METADATA_FILE_PATH,
    MODEL_NAME, 
    MODEL_DIR,
    BQ_DATASET_NAME,
    BQ_MOVIE_REVIEWS_TABLE
    )

# Start Spark with Spark NLP
# spark = sparknlp.start()

# Initialize Spark session
spark = get_spark(name="Sentiment Analysis of Movie Reviews")

def download_and_prep_sentiment_model() -> None:
    """
    Downloads the sentiment model from HuggingFace, 
    prepares it, and saves it to a GCS bucket.
    
    Args:
        None

    Returns:
        None
    """
  
    # Create a temporary directory with file paths for the tokenizer and model.
    temp_dir = tempfile.mkdtemp()
    tokenizer_path = os.path.join(temp_dir, "{}_tokenizer".format(MODEL_NAME))
    model_path = os.path.join(temp_dir, MODEL_NAME)

    # Download the tokenizer and model from HuggingFace.
    tokenizer = BertTokenizer.from_pretrained(MODEL_NAME)
    model = TFBertForSequenceClassification.from_pretrained(MODEL_NAME)
    
    # Save the tokenizer to the temporary directory.
    tokenizer.save_pretrained(tokenizer_path)
    
    # Define a TensorFlow serving function for the model.
    @tf.function(
    input_signature=[
        {
            "input_ids": tf.TensorSpec((None, None), tf.int32, name="input_ids"),
            "attention_mask": tf.TensorSpec((None, None), tf.int32, name="attention_mask"),
            "token_type_ids": tf.TensorSpec((None, None), tf.int32, name="token_type_ids"),
        }
    ]
    )
    def serving_fn(input):
        return model(input)

    # Save the model with the serving function to the temporary directory.
    model.save_pretrained(model_path, saved_model=True, signatures={"serving_default": serving_fn})
    
    # Create a reference path to the model's assets directory.
    # Next we'd need the tokenizer's vocab file and model's labels file
    # to be in the model's assets directory
    asset_path = os.path.join(model_path, "saved_model/1/assets") 

    # Copy the tokenizer's vocab file to the model's assets directory.
    shutil.copy(os.path.join(tokenizer_path, "vocab.txt"), asset_path)

    # Write the model's labels to a file in the assets directory.
    labels = model.config.label2id
    labels = sorted(labels, key=labels.get)
    with open(os.path.join(asset_path, "labels.txt"), "w") as f:
        f.write("\n".join(labels))

    # Load the saved model with assets into Spark NLP.
    sentimentClassifier = BertForSequenceClassification.loadSavedModel(
         os.path.join(model_path, 'saved_model/1'),
         spark)\
            .setInputCols(["document",'token'])\
            .setOutputCol("class")\
            .setCaseSensitive(True)\
            .setMaxSentenceLength(512)

    # Save the Spark NLP model to a GCS bucket.
    sentimentClassifier.write().overwrite().save(MODEL_DIR)

    # Clean up by removing the temporary directory.
    shutil.rmtree(temp_dir)

def main() -> None:
    """
    Orchestrates the sentiment analysis process on movie reviews.

    The function performs the following steps:
    1. Checks if the sentiment model is already saved in GCS.
    2. If not, downloads and prepares the sentiment model.
    3. Sets up a Spark NLP pipeline and applies it to the movie reviews dataset.
    4. Processes the resulting DataFrame to extract and format the sentiment results.
    5. Save the resulting DataFrame to BigQuery

    Args:
        None

    Returns:
        None
    """
    
    # Check if the sentiment model has already saved to GCS in a previous run
    if not check_file_exists(GCS_BUCKET, MODEL_DIR):
        download_and_prep_sentiment_model()
    
    # Create Spark NLP pipeline with the model
    document_assembler = (DocumentAssembler()
        .setInputCol('text')
        .setOutputCol('document'))

    tokenizer = (Tokenizer()
        .setInputCols(['document'])
        .setOutputCol('token'))

    sentiment_classifier = (BertForSequenceClassification.load(MODEL_DIR)
        .setInputCols(["document", 'token'])
        .setOutputCol("sentiment"))

    finisher = (Finisher()
        .setInputCols("sentiment")
        .setOutputCols("sentiment"))

    pipeline = Pipeline(stages=[
        document_assembler,
        tokenizer,
        sentiment_classifier,
        finisher
    ])

    # Define the schema for the movies
    schema = StructType([
        StructField("cid", IntegerType(), True),
        StructField("review_str", StringType(), True),
        StructField("id_review", IntegerType(), True)
    ])

    # Specify the read options
    read_options = {
        "schema": schema,
        "header": "true"
    }

    # Load movie reviews file(s) from GCS
    dataframes, new_files = load_files_from_gcs(
        spark=spark,
        bucket_name=GCS_BUCKET, 
        directory_path=MOVIE_FILES, 
        metadata_file_path=MOVIES_METADATA_FILE_PATH,
        file_format="csv",
        read_options=read_options
    )

    for movie_df in dataframes:
        # Rename the review_str column to match what the Spark NLP pipeline expects
        reviews = movie_df.withColumnRenamed("review_str", "text")

        # Apply the Spark NLP pipeline to the movie 'reviews' DataFrame 
        # to classify the sentiments of each review.
        result_df = pipeline.fit(reviews).transform(reviews)

        # Convert the resulting sentiment column values from arrays to strings  
        result_df = result_df.withColumn("sentiment", F.concat_ws(",", "sentiment"))
        
        # Create a positive_reviews column with boolean values 
        # 0 (negative) and 1 (positive)
        result_df = (result_df
            .withColumn(
                "positive_review", 
                F.when(result_df.sentiment == "negative", 0)
            .otherwise(1)))

        # Rename columns and select required columns
        result_df = (result_df
            .withColumnRenamed("cid", "user_id")
            .withColumnRenamed("id_review", "review_id")
            .withColumn("insert_date", F.current_timestamp())
            .select("user_id", "positive_review", "review_id", "insert_date"))
        
        # Save the DataFrame to BigQuery
        (result_df.write.format("bigquery")
            .option("temporaryGcsBucket", GCS_BUCKET)
            # .option("partitionField", "insert_date")
            .mode("append")
            .save(f"{BQ_DATASET_NAME}.{BQ_MOVIE_REVIEWS_TABLE}")
            )
    
    # After successful load into BigQuery, add the processed file 
    # to the metadata file
    for file in new_files:
        update_processed_files(GCS_BUCKET, MOVIES_METADATA_FILE_PATH, file)

if __name__ == "__main__":
    main()