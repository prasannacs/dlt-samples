# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG financials_reported;
# MAGIC USE SCHEMA company_facts;

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
import json

df = spark.read.option("multiline","true").option("mergeSchema","true").json("gs://what-a-bucket/sec-datasets/cf-schema.json")
json_schema_str = df.schema.json()
my_schema = StructType.fromJson(json.loads(json_schema_str))

df1 = spark.read.schema(my_schema).option("singleline","true").json("gs://what-a-bucket/sec-datasets/companyfacts/")
df2 = df1.select("cik","entityName","facts.us-gaap.*")
df2.write.saveAsTable("balance_sheet_bronze_8")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE balance_sheet_bronze_8

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

df = spark.read.table("financials_reported.company_facts.balance_sheet_bronze_8")
df1 = df.select("cik","entityName",explode("Assets.units.USD").alias("X"))
df2 = df1.select("cik","entityName",col("X.accn"), col("X.end").cast(DateType()).alias("end"), col("X.filed").cast(DateType()).alias("filed"), col("X.form"), col("X.fp"), col("X.frame"), col("X.fy"), col("X.val"))
display(df2)
#df2.write.saveAsTable("CostOfGoodsSold")
df2.createOrReplaceTempView("Assets_V")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT entityName
# MAGIC FROM Assets_V
# MAGIC ORDER BY entityName

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO InventoryNet
# MAGIC USING InventoryNet_V
# MAGIC ON InventoryNet.cik = InventoryNet_V.cik AND InventoryNet.entityName = InventoryNet_V.entityName AND InventoryNetRevenues.accn = InventoryNet_V.accn
# MAGIC --WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT entityName
# MAGIC FROM AssetsCurrent
# MAGIC ORDER BY entityName

# COMMAND ----------


