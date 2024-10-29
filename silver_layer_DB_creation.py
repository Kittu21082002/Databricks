# Databricks notebook source
spark.sql("create database if not exists silver_globalretail")

# COMMAND ----------

spark.sql("show databases").show()

# COMMAND ----------

spark.sql("use silver_globalretail")

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()
