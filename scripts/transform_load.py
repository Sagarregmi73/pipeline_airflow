import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, md5, concat_ws, current_timestamp

def main():
    # Initialize Spark Session with PostgreSQL JDBC Jar
    spark = SparkSession.builder \
        .appName("KaggleCDC_ETL") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
        .getOrCreate()

    db_url = "jdbc:postgresql://postgres_db:5432/airflow"
    db_properties = {"user": "airflow", "password": "airflow", "driver": "org.postgresql.Driver"}

    # Read Raw Ingested Data 
    raw_file = "/opt/airflow/data/shopping_trends.csv"
    if not os.path.exists(raw_file):
        print(f"Error: Ingested file {raw_file} not found!")
        return

    raw_df = spark.read.csv(raw_file, header=True, inferSchema=True)

    # Clean column names (replace spaces with underscores, lowercase)
    for col_name in raw_df.columns:
        raw_df = raw_df.withColumnRenamed(col_name, col_name.lower().replace(" ", "_"))

    # CDC Logic: Build a unique row hash signature based on changeable attributes
    cleaned_df = raw_df.dropna(subset=["customer_id"]) \
        .withColumn("row_hash", md5(concat_ws("-", "item_purchased", "purchase_amount_(usd)", "size"))) \
        .withColumn("processed_at", current_timestamp())

    # Optimize and look for deltas by comparing against the production warehouse
    try:
        existing_df = spark.read.jdbc(url=db_url, table="kaggle_silver.dim_customers", properties=db_properties) \
            .select("customer_id", col("row_hash").alias("old_hash"))
        
        # Keep only completely new rows or rows where data has mutated
        incremental_df = cleaned_df.join(existing_df, on="customer_id", how="left") \
            .filter((col("old_hash").isNull()) | (col("row_hash") != col("old_hash"))) \
            .drop("old_hash")
    except Exception:
        # Initial run if table doesn't exist yet
        incremental_df = cleaned_df

    # Overwrite the temporary Bronze staging buffer layer
    incremental_df.write.jdbc(
        url=db_url, 
        table="public.staging_orders", 
        mode="overwrite", 
        properties=db_properties
    )
    print("Success: Incremental delta staged into public.staging_orders.")
    spark.stop()

if __name__ == "__main__":
    main()
