# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG financials_reported;
# MAGIC USE SCHEMA company_facts;

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
import json

df = spark.read.option("multiline","true").json("gs://what-a-bucket/sec-datasets/facts/companyfacts/dir_002/CIK0000011544.json")
json_schema_str = df.schema.json()
my_schema = StructType.fromJson(json.loads(json_schema_str))

df1 = spark.read.schema(my_schema).option("singleline","true").json("gs://what-a-bucket/sec-datasets/companyfacts/")
df2 = df1.select("cik","entityName","facts.us-gaap.*")
df2.write.saveAsTable("balance_sheet_bronze")

# COMMAND ----------


