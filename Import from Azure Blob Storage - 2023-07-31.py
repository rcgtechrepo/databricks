# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC This notebook shows you how to create and query a table or DataFrame loaded from data stored in Azure Blob storage.

# COMMAND ----------

# MAGIC %who_ls
# MAGIC
# MAGIC %macro my_macro 1-3 4
# MAGIC
# MAGIC %pwd
# MAGIC a = _
# MAGIC print('--> ' + str(a))
# MAGIC
# MAGIC # %run -d -b40 myscript
# MAGIC
# MAGIC %sc a=ls
# MAGIC display(a)
# MAGIC
# MAGIC %set_env var=VAL
# MAGIC
# MAGIC %env var

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 1: Set the data location and type
# MAGIC
# MAGIC There are two ways to access Azure Blob storage: account keys and shared access signatures (SAS).
# MAGIC
# MAGIC To get started, we need to set the location and type of the file.

# COMMAND ----------

storage_account_name = "rcgsynacct"
storage_account_access_key = "nYz8KBLsqN5cGjUyX13NNELmN4Z7wYoPwU/L223O/ijahAEhOoYjWR49sXY0xobqUYO66MWVTtEF+AStmiwtPw=="

# COMMAND ----------

file_location = "wasbs://synfsname@rcgsynacct.blob.core.windows.net/2023/NYCTripSmall_3.parquet"
file_type = "parquet"

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  "nYz8KBLsqN5cGjUyX13NNELmN4Z7wYoPwU/L223O/ijahAEhOoYjWR49sXY0xobqUYO66MWVTtEF+AStmiwtPw==")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 2: Read the data
# MAGIC
# MAGIC Now that we have specified our file metadata, we can create a DataFrame. Notice that we use an *option* to specify that we want to infer the schema from the file. We can also explicitly set this to a particular schema if we have one already.
# MAGIC
# MAGIC First, let's create a DataFrame in Python.

# COMMAND ----------

df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)
print(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 3: Query the data
# MAGIC
# MAGIC Now that we have created our DataFrame, we can query it. For instance, you can identify particular columns to select and display.

# COMMAND ----------

display(df.select("VendorID"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 4: (Optional) Create a view or table
# MAGIC
# MAGIC If you want to query this data as a table, you can simply register it as a *view* or a table.

# COMMAND ----------

df.createOrReplaceTempView("TEMP_VIEW_NAME")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We can query this view using Spark SQL. For instance, we can perform a simple aggregation. Notice how we can use `%sql` to query the view from SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT tpep_pickup_datetime, SUM(VendorID) FROM TEMP_VIEW_NAME GROUP BY tpep_pickup_datetime

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Since this table is registered as a temp view, it will be available only to this notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.

# COMMAND ----------

df.write.format("parquet").mode("overwrite").saveAsTable("MY_PERMANENT_TABLE_NAME")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This table will persist across cluster restarts and allow various users across different notebooks to query this data.
