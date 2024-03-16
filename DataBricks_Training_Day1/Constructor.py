# Databricks notebook source
from pyspark.sql.functions import *
df=spark.read.json("dbfs:/FileStore/tables/formula1/constructors.json")

# COMMAND ----------

df.write.parquet("dbfs:/FileStore/tables/processed/formula1/")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
