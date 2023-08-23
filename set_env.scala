// Databricks notebook source
// MAGIC %md
// MAGIC #### Set the  storage account confirguration
// MAGIC
// MAGIC [Core Config Ref](https://docs.databricks.com/_static/notebooks/data-sources/mount-azure-blob-storage.html)

// COMMAND ----------

// MAGIC %run ./base 

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("dbfs:/FileStore/jars/660f96a3_d9bf_4276_8ac9_6acf81843b30/")
// MAGIC dbutils.fs.cp("dbfs:/FileStore/jars/e7fea824_3602_4d6d_9c29_2fa62a1bc5b4/testWheel-1.2-py2.py3-none-any.whl", "file:/tmp")

// COMMAND ----------

// MAGIC %sh
// MAGIC # /databricks/python/bin/pip freeze
// MAGIC cd /tmp
// MAGIC python -m pip install --upgrade pip
// MAGIC rm -rf /local_disk0/.ephemeral_nfs/envs/pythonEnv-a2ca3275-e214-439b-b064-0eb20363e9c2/lib/python3.9/site-packages/testWheel*
// MAGIC pip install testWheel-1.2-py2.py3-none-any.whl --force
// MAGIC ls /local_disk0/.ephemeral_nfs/envs/pythonEnv-a2ca3275-e214-439b-b064-0eb20363e9c2/lib/python3.9/site-packages

// COMMAND ----------

// MAGIC %python
// MAGIC #from common_modules import controls as controls
// MAGIC from common_modules import logging as lg
// MAGIC from testWheel import main
// MAGIC
// MAGIC lg.setup_logging("this")
// MAGIC main.main()
// MAGIC
// MAGIC @decodecorator(str, "Decorator for 'stringJoin'", "stringJoin started ...")
// MAGIC def stringJoin(*args, name, type):
// MAGIC     st = ''
// MAGIC     for i in args:
// MAGIC         st += i
// MAGIC     return st
// MAGIC     
// MAGIC print(stringJoin("I ", 'like ', "Geeks", 'for', "geeks", name="XXXX", type="type"))

// COMMAND ----------

val containerName = "test1"
val storageAccountName = "cs210037ffea9c979dd"
val sas = "?sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2030-08-07T03:37:49Z&st=2022-08-06T19:37:49Z&spr=https&sig=D0mfCr9AHfV7Wx9FByZweOiAgvsGrC5fo%2FSKArRZtUo%3D"
val config = "fs.azure.sas." + containerName+ "." + storageAccountName + ".blob.core.windows.net"

// COMMAND ----------

// MAGIC %md
// MAGIC #### Mount the storage account blob container
// MAGIC
// MAGIC [Core Config Ref](https://docs.databricks.com/_static/notebooks/data-sources/mount-azure-blob-storage.html)

// COMMAND ----------

if (!dbutils.fs.mounts.map(mnt => mnt.mountPoint).contains("/mnt/test1"))
dbutils.fs.mount(
  source = "wasbs://test1@cs210037ffea9c979dd.blob.core.windows.net",
  mountPoint = "/mnt/test1",
  extraConfigs = Map(config -> sas))

// COMMAND ----------

// MAGIC %md
// MAGIC #### Copy the prepared archive from the mounted container to the Databricks node
// MAGIC
// MAGIC [Core Config Ref](https://docs.databricks.com/_static/notebooks/dbutils.html)

// COMMAND ----------

dbutils.fs.cp("dbfs:/mnt/test1/sdk_linux_x64_83_dll.tar.gz", "file:/tmp")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Extract the preapred archive
// MAGIC
// MAGIC [Core Config Ref](https://docs.python.org/3/library/tarfile.html)

// COMMAND ----------

// MAGIC %python
// MAGIC import tarfile
// MAGIC
// MAGIC # pre-built libraries have been copied into /tmp/sdk_
// MAGIC file = tarfile.open('/tmp/sdk_linux_x64_83_dll.tar.gz')
// MAGIC
// MAGIC file.extractall('/tmp/')
// MAGIC
// MAGIC file.close()
// MAGIC
// MAGIC #import zipfilef
// MAGIC #with zipfile.ZipFile("/tmp/sdk_linux_x64_83_dll.zip", 'r') as zip_ref:
// MAGIC #    zip_ref.extractall("/tmp/sdk_linux_x64_83_dll")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Copy the Java Drawing SDK archive from the mounted container to the Databricsk node
// MAGIC
// MAGIC [Core Config Ref](https://docs.databricks.com/_static/notebooks/dbutils.html)

// COMMAND ----------

dbutils.fs.cp("dbfs:/mnt/test1/Drawings.Java.tar.gz", "file:/tmp")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Extract the Drawings archive
// MAGIC
// MAGIC [Core Config Ref](https://docs.python.org/3/library/tarfile.html)

// COMMAND ----------

// MAGIC %python
// MAGIC import tarfile
// MAGIC
// MAGIC file = tarfile.open('/tmp/Drawings.Java.tar.gz')
// MAGIC
// MAGIC file.extractall('/tmp/')
// MAGIC
// MAGIC file.close()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Copy your custom jar from the mounted container to the Databricks node
// MAGIC
// MAGIC [Core Config Ref](https://docs.databricks.com/_static/notebooks/dbutils.html)

// COMMAND ----------

dbutils.fs.cp("dbfs:/mnt/test1/OdReadExJava1.jar", "file:/tmp/Drawings.Java/bin/lnxX64_8.3dll")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Copy the Teigha jars from extracted archive to the java libarary path
// MAGIC
// MAGIC [Core Config Ref](https://stackoverflow.com/questions/29968292/what-is-java-library-path-set-to-by-default)

// COMMAND ----------

dbutils.fs.cp("file:/tmp/sdk_linux_x64_83_dll/lib/lnxX64_8.3dll", "file:/usr/lib", true)
dbutils.fs.cp("file:/tmp/sdk_linux_x64_83_dll/bin/lnxX64_8.3dll", "file:/usr/lib", true)
dbutils.fs.cp("file:/tmp/Drawings.Java/bin/lnxX64_8.3dll", "file:/usr/lib", true)
