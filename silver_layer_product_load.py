# Databricks notebook source
spark.sql("use silver_globalretail")
spark.sql("""
CREATE TABLE IF NOT EXISTS silver_products (
    product_id STRING,
    name STRING,
    category STRING,
    brand STRING,
    price DOUBLE,
    stock_quantity INT,
    rating DOUBLE,
    is_active BOOLEAN,
    price_category STRING,
    stock_status STRING,
    last_updated TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

last_processed_df = spark.sql("SELECT MAX(last_updated) as last_processed FROM silver_products")
last_processed_timestamp = last_processed_df.collect()[0]['last_processed']

if last_processed_timestamp is None:
    last_processed_timestamp = "1900-01-01T00:00:00.000+00:00"

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW bronze_incremental_products AS
SELECT *
FROM globalretail_bronze.bronze_product_delta WHERE ingestion_timestamp > '{last_processed_timestamp}'

""")

# COMMAND ----------

# MAGIC %md
# MAGIC Data Transformations:
# MAGIC    - Price normalization (setting negative prices to 0)
# MAGIC    - Stock quantity normalization (setting negative stock to 0)
# MAGIC    - Rating normalization (clamping between 0 and 5)
# MAGIC    - Price categorization (Premium, Standard, Budget)
# MAGIC    - Stock status calculation (Out of Stock, Low Stock, Moderate Stock, Sufficient Stock)
# MAGIC

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW silver_incremental_products AS
SELECT
    product_id,
    name,
    category,
    brand,
    CASE
        WHEN price < 0 THEN 0
        ELSE price
    END AS price,
    CASE
        WHEN stock_quantity < 0 THEN 0
        ELSE stock_quantity
    END AS stock_quantity,
    CASE
        WHEN rating < 0 THEN 0
        WHEN rating > 5 THEN 5
        ELSE rating
    END AS rating,
    is_active,
    CASE
        WHEN price > 1000 THEN 'Premium'
        WHEN price > 100 THEN 'Standard'
        ELSE 'Budget'
    END AS price_category,
    CASE
        WHEN stock_quantity = 0 THEN 'Out of Stock'
        WHEN stock_quantity < 10 THEN 'Low Stock'
        WHEN stock_quantity < 50 THEN 'Moderate Stock'
        ELSE 'Sufficient Stock'
    END AS stock_status,
    CURRENT_TIMESTAMP() AS last_updated
FROM bronze_incremental_products
WHERE name IS NOT NULL AND category IS NOT NULL
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_incremental_products

# COMMAND ----------

spark.sql("""
MERGE INTO silver_products target
USING silver_incremental_products source
ON target.product_id = source.product_id
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  silver_products
