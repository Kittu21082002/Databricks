# Databricks notebook source
spark.sql("USE silver_globalretail")
spark.sql("""
CREATE TABLE IF NOT EXISTS silver_orders (
    transaction_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    total_amount DOUBLE,
    transaction_date DATE,
    payment_method STRING,
    store_type STRING,
    order_status STRING,
    last_updated TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

# Get the last processed timestamp from silver layer
last_processed_df = spark.sql("SELECT MAX(last_updated) as last_processed FROM silver_orders")
last_processed_timestamp = last_processed_df.collect()[0]['last_processed']

if last_processed_timestamp is None:
    last_processed_timestamp = "1900-01-01T00:00:00.000+00:00"

# COMMAND ----------

# Create a temporary view of incremental bronze data
spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW bronze_incremental_orders AS
SELECT *
FROM globalretail_bronze.bronze_transaction_delta WHERE ingestion_timestamp > '{last_processed_timestamp}'

""")

# COMMAND ----------

spark.sql("select * from bronze_incremental_orders").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Data Transformations:
# MAGIC    - Quantity and total_amount normalization (setting negative values to 0)
# MAGIC    - Date casting to ensure consistent date format
# MAGIC    - Order status derivation based on quantity and total_amount
# MAGIC
# MAGIC Data Quality Checks: We filter out records with null transaction dates, customer IDs, or product IDs.
# MAGIC

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW silver_incremental_orders AS
SELECT
    transaction_id,
    customer_id,
    product_id,
    CASE 
        WHEN quantity < 0 THEN 0 
        ELSE quantity 
    END AS quantity,
    CASE 
        WHEN total_amount < 0 THEN 0 
        ELSE total_amount 
    END AS total_amount,
    CAST(transaction_date AS DATE) AS transaction_date,
    payment_method,
    store_type,
    CASE
        WHEN quantity = 0 OR total_amount = 0 THEN 'Cancelled'
        ELSE 'Completed'
    END AS order_status,
    CURRENT_TIMESTAMP() AS last_updated
FROM bronze_incremental_orders
WHERE transaction_date IS NOT NULL
  AND customer_id IS NOT NULL
  AND product_id IS NOT NULL
""")

# COMMAND ----------

spark.sql("select * from silver_incremental_orders").show()

# COMMAND ----------

spark.sql("""
MERGE INTO silver_orders target
USING silver_incremental_orders source
ON target.transaction_id = source.transaction_id
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_orders
