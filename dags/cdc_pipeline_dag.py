from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
}

with DAG(
    'kaggle_postgres_cdc_pipeline',
    default_args=default_args,
    description='Multi-layered Medallion Warehouse pipeline using PySpark and Postgres',
    schedule_interval=None, 
    catchup=False,
) as dag:

    # 1. BRONZE LAYER INGESTION: Download Kaggle Dataset
    download_data = BashOperator(
        task_id='download_kaggle_data',
        bash_command='kaggle datasets download -d jamshedkhan/shopping-trends-dataset -p /opt/airflow/data/ --unzip',
    )

    # 2. COMPUTE PROCESSING: Run PySpark CDC Analysis
    run_spark_cdc = BashOperator(
        task_id='run_pyspark_cdc',
        bash_command='spark-submit --jars /opt/spark/jars/postgresql-42.6.0.jar /opt/airflow/scripts/transform_load.py',
    )

    # 3. WAREHOUSE STORAGE: Build Layered Schemas and perform Idempotent Upsert
    postgres_upsert = SQLExecuteQueryOperator(
        task_id='postgres_upsert_merge',
        conn_id='postgres_db',  
        sql="""
            -- Create Medallion Schemas
            CREATE SCHEMA IF NOT EXISTS kaggle_bronze;
            CREATE SCHEMA IF NOT EXISTS kaggle_silver;
            CREATE SCHEMA IF NOT EXISTS kaggle_gold;

            -- Relocate newly written staging data into Bronze Schema
            ALTER TABLE IF EXISTS public.staging_orders SET SCHEMA kaggle_bronze;

            -- Establish Modeled Silver Layer Core Dimension Table
            CREATE TABLE IF NOT EXISTS kaggle_silver.dim_customers (
                customer_id INT PRIMARY KEY,
                age INT,
                gender VARCHAR(15),
                item_purchased VARCHAR(100),
                category VARCHAR(50),
                purchase_amount__usd_ NUMERIC(10,2),
                location VARCHAR(100),
                size VARCHAR(10),
                color VARCHAR(30),
                season VARCHAR(20),
                review_rating NUMERIC(3,1),
                subscription_status VARCHAR(20),
                previous_purchases INT,
                row_hash VARCHAR(32),
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Optimise analytical query paths via indexing
            CREATE INDEX IF NOT EXISTS idx_dim_customers_lookup 
            ON kaggle_silver.dim_customers (location, category);

            -- Run high-speed relational SQL Upsert Merge
            INSERT INTO kaggle_silver.dim_customers (
                customer_id, age, gender, item_purchased, category, purchase_amount__usd_, 
                location, size, color, season, review_rating, subscription_status, 
                previous_purchases, row_hash, processed_at
            )
            SELECT 
                customer_id, age, gender, item_purchased, category, purchase_amount__usd_, 
                location, size, color, season, review_rating, subscription_status, 
                previous_purchases, row_hash, processed_at
            FROM kaggle_bronze.staging_orders
            ON CONFLICT (customer_id) 
            DO UPDATE SET 
                item_purchased = EXCLUDED.item_purchased,
                purchase_amount__usd_ = EXCLUDED.purchase_amount__usd_,
                size = EXCLUDED.size,
                row_hash = EXCLUDED.row_hash,
                processed_at = EXCLUDED.processed_at
            WHERE kaggle_silver.dim_customers.row_hash IS DISTINCT FROM EXCLUDED.row_hash;

            -- Establish Gold Layer Analytical Aggregate Mart View
            CREATE OR REPLACE VIEW kaggle_gold.fact_sales_summary AS
            SELECT 
                location,
                category,
                COUNT(customer_id) as total_orders,
                SUM(purchase_amount__usd_) as total_revenue,
                AVG(review_rating) as average_customer_rating,
                MAX(processed_at) as last_warehouse_sync_time
            FROM kaggle_silver.dim_customers
            GROUP BY location, category;
        """,
    )

    download_data >> run_spark_cdc >> postgres_upsert
