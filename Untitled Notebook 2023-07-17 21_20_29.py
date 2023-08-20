# Databricks notebook source
# MAGIC %sql
# MAGIC CACHE SELECT VendorID, tpep_pickup_datetime FROM my_permanent_table_name  WHERE VendorID > 0; 
# MAGIC
# MAGIC CACHE TABLE my_permanent_table_name OPTIONS ('storageLevel' 'DISK_ONLY') SELECT * FROM my_permanent_table_name;
# MAGIC

# COMMAND ----------

print(sc._jvm.java.util.Random().nextInt(10))
new_dataframe_name = _sqldf
display(new_dataframe_name)
new_dataframe_name.printSchema()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
account_name = "rcgsynacct"
container_name = "synfsname"
relative_path = "2023/NYCTripSmall_3.parquet"
adls_path = 'abfss://%s@%s.dfs.core.windows.net/%s' % (container_name, account_name, relative_path)
print(adls_path)
df1 = spark.read.parquet(adls_path)
df1.printSchema
df1.show(4)


# COMMAND ----------

# dbutils.fs.unmount('/mnt/synfsname')
containerName = 'synfsname'
storageAccountName = 'rcgsynacct'
sas = '?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2030-07-30T12:46:03Z&st=2023-07-30T04:46:03Z&spr=https&sig=GTooN4ckvcSuyNXYJeisIIou4bGDJdLNhVnMmlZN8kw%3D'
config = f'fs.azure.sas.{containerName}.{storageAccountName}.blob.core.windows.net'
mount_point = '/mnt/synfsname'
mounted = any(mount_point in d for d in dbutils.fs.mounts())
if not mounted :
    print(f'{mount_point} not in mounts.. mounting now..')
    dbutils.fs.mount(
    source = f'wasbs://{containerName}@{storageAccountName}.blob.core.windows.net',
    mount_point  = mount_point,
    extra_configs = {config : sas})
else :
    print(f'{mount_point} already mounted')

dbutils.fs.ls(mount_point)
dbutils.fs.cp(f'dbfs:{mount_point}/2023/NYCTripSmall_3.parquet', 'file:/tmp')


# COMMAND ----------

# MAGIC %scala
# MAGIC val containerName = "synfsname"
# MAGIC val storageAccountName = "rcgsynacct"
# MAGIC val sas = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2030-07-30T12:46:03Z&st=2023-07-30T04:46:03Z&spr=https&sig=GTooN4ckvcSuyNXYJeisIIou4bGDJdLNhVnMmlZN8kw%3D"
# MAGIC val config = "fs.azure.sas." + containerName+ "." + storageAccountName + ".blob.core.windows.net"

# COMMAND ----------

# MAGIC %scala
# MAGIC if (!dbutils.fs.mounts.map(mnt => mnt.mountPoint).contains("/mnt/synfsname"))
# MAGIC dbutils.fs.mount(
# MAGIC   source = "wasbs://synfsname@rcgsynacct.blob.core.windows.net",
# MAGIC   mountPoint = "/mnt/synfsname",
# MAGIC   extraConfigs = Map(config -> sas))

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.fs.ls("/mnt/synfsname")
# MAGIC dbutils.fs.cp("dbfs:/mnt/synfsname/2023/NYCTripSmall_3.parquet", "file:/tmp")

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp/

# COMMAND ----------

df = spark.read.parquet('/mnt/synfsname/2023/NYCTripSmall_3.parquet')
df.schema
df.show(2)

# COMMAND ----------

from common_modules import logging as lg
lg.log_message(None, "", "", "", "", "", "", "")

# COMMAND ----------

exec_script = """
print("test")
"""

dbutils.fs.put("/tmp/exec_script.py", exec_script, True)

# COMMAND ----------

dbutils.fs.ls("/tmp/exec_script.py")

# COMMAND ----------

dbutils.fs.head("dbfs:/tmp/exec_script.py")

# COMMAND ----------

print(dbutils.widgets.get("country"))
print(dbutils.widgets.get("parameter"))
print(dbutils.widgets.get("subject"))
