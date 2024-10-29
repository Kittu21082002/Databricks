# Databricks notebook source
spark.sql("create database if not exists globalretail_gold")
spark.sql("use globalretail_gold")

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

spark.sql("select current_database()").show()
