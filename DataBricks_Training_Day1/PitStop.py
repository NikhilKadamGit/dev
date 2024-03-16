# Databricks notebook source
df=spark.read.json("dbfs:/FileStore/tables/formula1/pit_stops.json",multiLine=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.saveAsTable("nikhil.pitstop")

# COMMAND ----------


