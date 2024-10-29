# Databricks notebook source
filepath="dbfs:/FileStore/GlobalRetail/bronze_layer/product_catalog/products.json"
df=spark.read.json(filepath)
df.show()

# COMMAND ----------

#creating a new column in dataframe and with timestamp
from pyspark.sql.functions import current_timestamp
df_new=df.withColumn("ingestion_timestamp",current_timestamp())
display(df_new)

# COMMAND ----------

#creating a delta table and saving with mode append
spark.sql("use globalretail_bronze")
df_new.write.format("delta").mode("append").saveAsTable("bronze_product_delta")

# COMMAND ----------

spark.sql("select * from bronze_product_delta limit 10").show()

# COMMAND ----------

import datetime
archive_folder="dbfs:/FileStore/GlobalRetail/bronze_layer/product_catalog/archive/"
archive_filepath=archive_folder +"_"+datetime.datetime.now().strftime("%Y%M%d%H%m%s")
dbutils.fs.mv(filepath,archive_filepath)
print(archive_filepath)
