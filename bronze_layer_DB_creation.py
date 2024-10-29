# Databricks notebook source
spark.sql("CREATE DATABASE IF NOT EXISTS globalretail_bronze")

# COMMAND ----------

spark.sql("show databases").show()

# COMMAND ----------

spark.sql("use globalretail_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------


