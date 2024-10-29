# Databricks notebook source
spark.sql("USE silver_globalretail")
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver_customers (
    customer_id STRING,
    name STRING,
    email STRING,
    country STRING,
    customer_type STRING,
    registration_date DATE,
    age INT,
    gender STRING,
    total_purchases INT,
    customer_segment STRING,
    days_since_registration INT,
    last_updated TIMESTAMP)
""")

# COMMAND ----------

# Get the last processed timestamp from silver layer
last_processed_df = spark.sql("SELECT MAX(last_updated) as last_processed FROM silver_customers")
last_processed_timestamp = last_processed_df.collect()[0]['last_processed']

if last_processed_timestamp is None:
    last_processed_timestamp = "1900-01-01T00:00:00.000+00:00"

"""here, we are like data comes into DBFS and then next to bronze layer and that same data to silver
how means, if the data is same we need not load that last processed right so we use logic
that, first we load the data initially to silevr from bronze right ... so for next time loading we use 
rhe lastprocessed time of MAXIMUM and now time>last time 2024-10-23 > 2024-10-22 then we get data merged only"""

# COMMAND ----------

#Create a temporary view of incremental bronze data and loading bronze data like incremental load and timestamp based
spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW bronze_incremental AS
SELECT *
FROM globalretail_bronze.bronze_customer_delta c where  c.ingestion_timestamp > '{last_processed_timestamp}'
""")

# COMMAND ----------

spark.sql("select * from bronze_incremental").show()

# COMMAND ----------

#Validate email addresses (null or not null)
#Valid age between 18 to 100
#Create customer_segment as total_purchases > 10000 THEN 'High Value' if > 5000 THEN 'Medium Value'  ELSE 'Low Value'
#days since user is registered in the system
#Remove any junk records where total_purchase is negative number

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW silver_incremental AS
SELECT
    customer_id,
    name,
    email,
    country,
    customer_type,
    registration_date,
    age,
    gender,
    total_purchases,
    CASE
        WHEN total_purchases > 10000 THEN 'High Value'
        WHEN total_purchases > 5000 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_segment,
    DATEDIFF(CURRENT_DATE(), registration_date) AS days_since_registration,
    CURRENT_TIMESTAMP() AS last_updated
FROM bronze_incremental
WHERE 
    age BETWEEN 18 AND 100
    AND email IS NOT NULL
    AND total_purchases >= 0
""")

# COMMAND ----------

display(spark.sql("select * from silver_incremental"))

# COMMAND ----------

#merging data not appending into silvercustomer table
spark.sql("""
MERGE INTO silver_customers target
USING silver_incremental source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
""")


# COMMAND ----------

spark.sql("select count(*) from silver_customers").show()
