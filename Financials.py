# Databricks notebook source
from pyspark.sql.types import *

schema = StructType([
    StructField("startDate",StringType(),False),
    StructField("endDate",StringType(),False),
    StructField("year",StringType(),False),
    StructField("quarter",StringType(),False),
    StructField("symbol",StringType(),False),
    StructField("data",StructType([
        StructField("bs",ArrayType(StructType([
            StructField("label",StringType(),True),
            StructField("concept",StringType(),True),
            StructField("unit",StringType(),True),
            StructField("value",StringType(),True)
        ])

        ))
    ])),
])

df = spark.read.schema(schema).option("multiline","true").json("gs://what-a-bucket/Financials/202*/")
df.cache();
display(df);


# COMMAND ----------

from pyspark.sql.functions import *

schema =    StructType([
StructField("label",StringType(),True),
            StructField("concept",StringType(),True),
            StructField("unit",StringType(),True),
            StructField("value",StringType(),True)
])
df1 = df.select("startDate", "endDate", "year", "quarter", "symbol", explode("data.bs").alias("X"))
df2 = df1.select("startDate","endDate", "symbol", "year", "quarter", col("X.label"), col("X.concept"), col("X.unit"), col("X.value"))

df2.write.saveAsTable("records1")



# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG Financials;
# MAGIC USE SCHEMA balanceSheet;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, ROW_NUMBER() OVER(PARTITION BY value
# MAGIC         ORDER BY label) rank FROM records1 

# COMMAND ----------


