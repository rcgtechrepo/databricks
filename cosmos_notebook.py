# Databricks notebook source
# MAGIC %%pyspark
# MAGIC spark.sql("DROP TABLE default.test")
# MAGIC spark.sql("CREATE TABLE IF NOT EXISTS default.test (id Int, name String) USING Parquet LOCATION '/synfsname/parquet/'")
# MAGIC
# MAGIC values = [(1, "left"),(2, "right"),(3,"center")]
# MAGIC df = sqlContext.createDataFrame(values,['id','name'])
# MAGIC
# MAGIC df.write.mode('overwrite').format('parquet').saveAsTable('default.test')
# MAGIC df = spark.sql("SELECT * FROM default.test")
# MAGIC df.show(100)

# COMMAND ----------

from notebookutils import mssparkutils
mssparkutils.fs.help()

# COMMAND ----------

from pyspark.sql.functions import col

%timeit df = spark.read.format("cosmos.olap")\
    .option("spark.synapse.linkedService", "CosmosDbNoSql1")\
    .option("spark.cosmos.container", "metadata")\
    .load()

key = df.filter(df['id'] == '100')\
                .select("key")\
                .collect()[0]['key']

print(key)

# COMMAND ----------

# from pyspark.sql import SparkSession

# blob_account_name = 'rcgsynacct' 
# blob_container_name = 'synfsname'
# blob_relative_path = '2023/NYCTripSmall_3.parquet'
# linked_service_name = 'rcg-synapse-WorkspaceDefaultStorage'

# blob_sas_token = mssparkutils.credentials.getConnectionStringOrCreds(linked_service_name)

# wasb_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)

# spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token)
# print('Remote blob path: ' + wasb_path)

# df = spark.read.parquet(wasb_path)
# df.schema
# df.show(2)


from pyspark.sql import SparkSession
from pyspark.sql.types import *
account_name = "rcgsynacct"
container_name = "synfsname"
relative_path = "2023/NYCTripSmall_3.parquet"

# adls_path = 'abfss://%s@%s.dfs.core.windows.net/%s' % (container_name, account_name, relative_path)
adls_path = f'abfss://{container_name}@{account_name}.dfs.core.windows.net/{relative_path}'

print(adls_path)
df1 = spark.read.parquet(adls_path)
df1.printSchema
# df1.show(4)
