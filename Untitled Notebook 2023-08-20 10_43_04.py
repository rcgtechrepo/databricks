# Databricks notebook source
# MAGIC %scala
# MAGIC spark.conf.set("spark.databricks.io.cache.enabled", true)
# MAGIC import sqlContext.implicits._
# MAGIC import org.apache.spark.storage.StorageLevel
# MAGIC import org.apache.spark.sql.DataFrame
# MAGIC
# MAGIC val df = Seq(1, 2, 3).toDF()
# MAGIC
# MAGIC import sqlContext.implicits._
# MAGIC import org.apache.spark.storage.StorageLevel
# MAGIC import org.apache.spark.sql.DataFrame
# MAGIC def printStorageInfo(df: DataFrame): Unit = {
# MAGIC   // Print RDD storage level info
# MAGIC   println(s"RDD storageLevel: ${df.rdd.getStorageLevel}")
# MAGIC   println(s"RDD storageLevel is StorageLevel.NONE: ${df.rdd.getStorageLevel == StorageLevel.NONE}")
# MAGIC   // Print Dataframe storage level info
# MAGIC   println(s"DataFrame storageLevel: ${df.storageLevel}")
# MAGIC   println(s"DataFrame storageLevel is StorageLevel.NONE: ${df.storageLevel == StorageLevel.NONE}")
# MAGIC }
# MAGIC
# MAGIC printStorageInfo(df)
# MAGIC
# MAGIC df.cache()
# MAGIC
# MAGIC printStorageInfo(df)
# MAGIC df.unpersist()
# MAGIC
# MAGIC printStorageInfo(df)
