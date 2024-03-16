# Databricks notebook source
print("Hello World")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database Nikhil

# COMMAND ----------

data=([(1,'a',30),(2,'b',34)])
schema="id int, name string, age int"
df=spark.createDataFrame(data,schema)

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %fs.mkdirs("dbfs./FileStore/proc")

# COMMAND ----------

dbutils.fs.mkdirs("dbfs./FileStore/proc")

# COMMAND ----------

# MAGIC %fs ls dbfs./FileStore/proc

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/tables/raw/emp.csv")

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/raw/emp.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.emp_demp

# COMMAND ----------

df.write.saveAsTable("nikhil.emp_demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nikhil.emp_demo

# COMMAND ----------


