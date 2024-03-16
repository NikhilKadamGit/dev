# Databricks notebook source
df=spark.read.csv("dbfs:/FileStore/tables/formula1/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df.select("circuitId","name")

# COMMAND ----------

df.select("circuitId","name").display()

# COMMAND ----------

df.select("circuitId".alias("circuit_id"),"name").display()

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id"),"name").display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id"),"name").display()

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id"),"name").display()

# COMMAND ----------

df.select(concat("location",lit(" & "),"country").alias("location&county")).display()

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_id").display()

# COMMAND ----------

df.withColumn("country", upper("country")).display()

# COMMAND ----------

df.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

from pyspark.sql.functions import *
df1=df.withColumnRenamed("circuitId", "circuit_id").withColumnRenamed("circuitRef", "circuit_ref").withColumn("ingestion_Date",current_timestamp()).drop("url")

# COMMAND ----------

df1.write.saveAsTable("nikhil.circuits")

# COMMAND ----------


