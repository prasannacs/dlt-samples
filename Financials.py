# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG Financials;
# MAGIC USE SCHEMA balanceSheet;

# COMMAND ----------

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
# MAGIC SELECT label, 
# MAGIC            concept, 
# MAGIC            symbol,
# MAGIC            year,
# MAGIC            quarter, 
# MAGIC            ROW_NUMBER() OVER(PARTITION BY value, symbol, year, quarter
# MAGIC         ORDER BY label, concept) AS DuplicateCount
# MAGIC     FROM records1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from records1

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW Financials.balanceSheet.records2_v AS 
# MAGIC WITH BUFF ( label, 
# MAGIC            concept, 
# MAGIC            symbol,
# MAGIC            unit,
# MAGIC            value,
# MAGIC            startDate,
# MAGIC            endDate,
# MAGIC            year,
# MAGIC            quarter,  
# MAGIC     DuplicateCount )
# MAGIC AS (SELECT label, 
# MAGIC            concept, 
# MAGIC            symbol,
# MAGIC            unit,
# MAGIC            cast(value as DOUBLE),
# MAGIC            cast(startDate as DATE),
# MAGIC            cast(endDate as DATE),
# MAGIC            year,
# MAGIC            quarter, 
# MAGIC            ROW_NUMBER() OVER(PARTITION BY value, symbol, year, quarter
# MAGIC         ORDER BY label, concept) AS DuplicateCount
# MAGIC     FROM Financials.balanceSheet.records1)
# MAGIC SELECT * FROM buff;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Financials.balanceSheet.records3 AS 
# MAGIC (
# MAGIC   SELECT label, 
# MAGIC            concept, 
# MAGIC            symbol,
# MAGIC            year,
# MAGIC            startDate,
# MAGIC            endDate,
# MAGIC            unit,
# MAGIC            value,
# MAGIC            quarter
# MAGIC   FROM Financials.balanceSheet.records2_v
# MAGIC   WHERE DuplicateCount = 1
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT label as label_l, concept as concept_l, symbol as symbol_l, year as year_l, unit as unit_l, value as value_l, quarter as quarter_l FROM records3 where label = 'Total liabilities'

# COMMAND ----------


