# Databricks notebook source
spark.sql("USE globalretail_gold")
spark.sql("""
CREATE OR REPLACE TABLE gold_daily_sales AS
SELECT 
    transaction_date,
    SUM(total_amount) AS daily_total_sales
FROM 
    silver_globalretail.silver_orders
GROUP BY 
    transaction_date
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_daily_sales
