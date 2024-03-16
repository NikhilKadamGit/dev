# Databricks notebook source
df2=spark.read.json("dbfs:/FileStore/tables/raw/iot1.json")

# COMMAND ----------

df2.write.saveAsTable("nikhil.iot_table")

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/raw

# COMMAND ----------

# MAGIC %run /Workspace/Users/nikhil.kadam@marsh.com/DataBricks_Training_Day1/includes

# COMMAND ----------

df2=spark.read.json(f"{input_path}iot1.json")

# COMMAND ----------


