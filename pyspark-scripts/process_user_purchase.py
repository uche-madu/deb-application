from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from google.cloud import storage
import argparse

def main(bucket_name, object_name):
    # Initialize Spark session
    spark = SparkSession.builder.appName("ProcessUserPurchaseData").getOrCreate()

    # Read CSV file from GCS
    df = spark.read.csv(f"gs://{bucket_name}/{object_name}", header=True, inferSchema=True)

    # Drop rows where customer_id is null
    df = df.dropna(subset=["CustomerID"])

    # Fill missing values with empty strings for object-type columns
    df = df.fillna("")

    # Rename columns
    df = df.withColumnRenamed("InvoiceNo", "invoice_number") \
           .withColumnRenamed("StockCode", "stock_code") \
           .withColumnRenamed("Description", "detail") \
           .withColumnRenamed("Quantity", "quantity") \
           .withColumnRenamed("InvoiceDate", "invoice_date") \
           .withColumnRenamed("UnitPrice", "unit_price") \
           .withColumnRenamed("CustomerID", "customer_id") \
           .withColumnRenamed("Country", "country")

    # Convert data types
    df = df.withColumn("invoice_number", col("invoice_number").cast("string")) \
           .withColumn("stock_code", col("stock_code").cast("string")) \
           .withColumn("detail", col("detail").cast("string")) \
           .withColumn("quantity", col("quantity").cast("int")) \
           .withColumn("invoice_date", col("invoice_date").cast("timestamp")) \
           .withColumn("unit_price", col("unit_price").cast("double")) \
           .withColumn("customer_id", col("customer_id").cast("int")) \
           .withColumn("country", col("country").cast("string"))

    # Write the cleaned data back to GCS or load directly into BigQuery
    output_path = f"gs://{bucket_name}/processed/user_purchase_cleaned"
    df.write.mode("overwrite").parquet(output_path)

    print("Data processing completed and saved to:", output_path)
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process User Purchase Data")
    parser.add_argument("--bucket_name", required=True, help="GCS bucket name")
    parser.add_argument("--object_name", required=True, help="Path to the CSV file in GCS")
    
    args = parser.parse_args()
    main(args.bucket_name, args.object_name)
