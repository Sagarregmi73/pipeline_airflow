import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, md5, concat_ws, current_timestamp

def main():
    pg_user = os.environ.get("POSTGRES_USER", "airflow")
    pg_pass = os.environ.get("POSTGRES_PASSWORD", "airflow")
    pg_db = os.environ.get("POSTGRES_DB", "airflow")
    
    db_url = f"jdbc:postgresql://postgres_db:5432/{pg_db}"
    db_properties = {
        "user": pg_user, 
        "password": pg_pass, 
        "driver": "org.postgresql.Driver"
    }

    spark = SparkSession.builder \
        .appName("KaggleCDC_ETL") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
        .getOrCreate()

    # Pre-creation initialization steps for Medallion Layer Schema Blocks
    try:
        init_spark = SparkSession.builder.getOrCreate()
        conn = init_spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(db_url, pg_user, pg_pass)
        stmt = conn.createStatement()
        stmt.execute("CREATE SCHEMA IF NOT EXISTS kaggle_bronze; CREATE SCHEMA IF NOT EXISTS kaggle_silver; CREATE SCHEMA IF NOT EXISTS kaggle_gold;")
        stmt.close()
        conn.close()
    except Exception as e:
        print(f"Schema initialization block note: {e}")

    raw_file = "/opt/airflow/data/shopping_trends.csv"
    if not os.path.exists(raw_file):
        print(f"Error: Ingested file {raw_file} not found!")
        return

    raw_df = spark.read.csv(raw_file, header=True, inferSchema=True)

    for col_name in raw_df.columns:
        raw_df = raw_df.withColumnRenamed(col_name, col_name.lower().replace(" ", "_"))

    cleaned_df = raw_df.dropna(subset=["customer_id"]) \
        .withColumn("row_hash", md5(concat_ws("-", "item_purchased", "purchase_amount__usd_", "size"))) \
        .withColumn("processed_at", current_timestamp())

    try:
        existing_df = spark.read.jdbc(url=db_url, table="kaggle_silver.dim_customers", properties=db_properties) \
            .select("customer_id", col("row_hash").alias("old_hash"))
        
        incremental_df = cleaned_df.join(existing_df, on="customer_id", how="left") \
            .filter((col("old_hash").isNull()) | (col("row_hash") != col("old_hash"))) \
            .drop("old_hash")
    except Exception:
        incremental_df = cleaned_df

    incremental_df.write.jdbc(
        url=db_url, 
        table="kaggle_bronze.staging_orders", 
        mode="overwrite", 
        properties=db_properties
    )
    print("Success: Incremental delta staged safely into kaggle_bronze.staging_orders.")
    spark.stop()

if __name__ == "__main__":
    main()
