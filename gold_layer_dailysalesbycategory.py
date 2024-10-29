# Databricks notebook source
spark.sql("USE globalretail_gold")
spark.sql("""
CREATE OR REPLACE TABLE gold_category_sales AS
SELECT 
    p.category AS product_category,
    SUM(o.total_amount) AS category_total_sales
FROM 
    silver_globalretail.silver_orders o
JOIN 
    silver_globalretail.silver_products p ON o.product_id = p.product_id
GROUP BY 
    p.category
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  gold_category_sales
