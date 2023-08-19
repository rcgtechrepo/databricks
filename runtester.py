# Databricks notebook source
# MAGIC %md
# MAGIC #### Read DWG File
# MAGIC
# MAGIC [Core Config Ref](https://kb.databricks.com/clusters/multiple-executors-single-worker.html)

# COMMAND ----------

# MAGIC %run ./set_env

# COMMAND ----------

# MAGIC %sh
# MAGIC for filename in /tmp/Drawings.Java/bin/lnxX64_8.3dll/*.dwg; do
# MAGIC   echo "$filename" 
# MAGIC done

# COMMAND ----------

exec_script = """
#!/bin/bash
ls -alt
"""

dbutils.fs.put("/tmp/exec_script.sh", exec_script, True)


# COMMAND ----------

dbutils.fs.ls("/tmp/exec_script.sh")


# COMMAND ----------

# MAGIC %sh
# MAGIC ls -alt

# COMMAND ----------

dbutils.fs.ls("/tmp/exec_script.sh")


# COMMAND ----------

dbutils.fs.ls("dbfs:/tmp/exec_script.sh")

# COMMAND ----------

# MAGIC %sh
# MAGIC cp /dbfs/tmp/exec_script.sh /readDWG
# MAGIC chmod a+x readDWG

# COMMAND ----------

dbutils.widgets.text("inPath", "/tmp/Drawings.Java/bin/lnxX64_8.3dll/", "inPath")
dbutils.widgets.text("outPath", "/tmp/Drawings.Java/bin/lnxX64_8.3dll/out", "outPath")
dbutils.widgets.text("numFiles", "56789", "numFiles")

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val inPath = dbutils.widgets.get("inPath")
# MAGIC val outPath = dbutils.widgets.get("outPath")
# MAGIC val numFiles = dbutils.widgets.get("numFiles")
# MAGIC val inputs = Array(
# MAGIC   Array(inPath, outPath, numFiles)
# MAGIC   )

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC def convertStr(input:Array[String])={
# MAGIC   /* Converts input order and issue id values to comma separated string*/
# MAGIC   val order = input(0)
# MAGIC   val issue = input(1)
# MAGIC   val factor = input(2)
# MAGIC   val input_val = " " + order + "," + issue + "," + factor
# MAGIC   input_val
# MAGIC }
