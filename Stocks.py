# Databricks notebook source
import dlt

@dlt.table
def stocks_bronze():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.useIncrementalListing", "false")
      .option("cloudFiles.inferColumnTypes", "false")
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      .load("gs://what-a-bucket/stocks3/")
  )

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt
import json

@dlt.table(
  comment="Silver table"
)
def stocks_silver():
  df = dlt.read("stocks_bronze")  
  dfc = df.select(explode(from_json("c", 'array<double>')).alias('Close_Prices'))
  dfh = df.select(explode(from_json("h", 'array<double>')).alias('High_Prices'))
  dfl = df.select(explode(from_json("l", 'array<double>')).alias('Low_Prices'))
  dfo = df.select(explode(from_json("o", 'array<double>')).alias('Open_Prices'))
  dft = df.select(explode(from_json("t", 'array<string>')).alias('Timestamp'))
  dfv = df.select(explode(from_json("v", 'array<int>')).alias('Volume'))
  dfs = df.select(explode(from_json("symbol", 'array<string>')).alias('Stock'))
  dff = df.select(explode(from_json("file", 'array<string>')).alias('File'))
  
  dfc = dfc.withColumn("row_id", monotonically_increasing_id())
  dfh = dfh.withColumn("row_id", monotonically_increasing_id())
  dfl = dfl.withColumn("row_id", monotonically_increasing_id())
  dfo = dfo.withColumn("row_id", monotonically_increasing_id())
  dft = dft.withColumn("row_id", monotonically_increasing_id())
  dfv = dfv.withColumn("row_id", monotonically_increasing_id())
  dfs = dfs.withColumn("row_id", monotonically_increasing_id())
  dff = dff.withColumn("row_id", monotonically_increasing_id())

  buff_df1 = dfc.join(dfh, ("row_id"))
  buff_df2 = buff_df1.join(dfl, ("row_id"))
  buff_df3 = buff_df2.join(dfo, ("row_id")) 
  buff_df4 = buff_df3.join(dft, ("row_id"))
  buff_df5 = buff_df4.join(dfv, ("row_id"))
  buff_df6 = buff_df5.join(dfs, ("row_id"))
  final_df = buff_df6.join(dff, ("row_id"))
   
  return final_df





# COMMAND ----------


