# Databricks notebook source
filepath="dbfs:/FileStore/GlobalRetail/bronze_layer/transaction_data/transaction_snappy.parquet"
df=spark.read.parquet(filepath)
df.show()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,col
new_df=df.withColumn("transaction_date",to_timestamp(col("transaction_date")))
new_df.printSchema()
new_df.show()

# COMMAND ----------

#creating a new column in dataframe and with timestamp
from pyspark.sql.functions import current_timestamp
final_df=new_df.withColumn("ingestion_timestamp",current_timestamp())
display(final_df)

# COMMAND ----------

#creating a delta table and saving with mode append
spark.sql("use globalretail_bronze")
final_df.write.format("delta").mode("append").saveAsTable("bronze_transaction_delta")

# COMMAND ----------

spark.sql("select * from bronze_transaction_delta limit 10").show()

# COMMAND ----------

import datetime
archive_folder="dbfs:/FileStore/GlobalRetail/bronze_layer/transaction_data/archive/"
archive_filepath=archive_folder +"_"+datetime.datetime.now().strftime("%Y%M%d%H%m%s")
dbutils.fs.mv(filepath,archive_filepath)
print(archive_filepath)
