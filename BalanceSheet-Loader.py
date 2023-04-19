# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG financials_reported;
# MAGIC USE SCHEMA balance_sheets;

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([
    StructField("cik",StringType(),False),
    StructField("symbol",StringType(),False),
    StructField("data",ArrayType(StructType([
        StructField("accessNumber",StringType(),False),
        StructField("cik",StringType(),False),
        StructField("year",StringType(),False),
        StructField("quarter",StringType(),False),
        StructField("form",StringType(),False),
        StructField("startDate",StringType(),False),
        StructField("endDate",StringType(),False),
        StructField("filedDate",StringType(),False),
        StructField("acceptedDate",StringType(),False),
        StructField("report",StructType([
            StructField("bs",ArrayType(StructType([
                StructField("label",StringType(),True),
                StructField("concept",StringType(),True),
                StructField("unit",StringType(),True),
                StructField("value",StringType(),True)
            ])
            ))
        ])),
    ]))),
])

df = spark.read.schema(schema).option("multiline","true").json("gs://what-a-bucket/SP100/")

#df=spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").option("cloudFiles.useIncrementalListing", "false").option("cloudFiles.inferColumnTypes", "false").option("cloudFiles.schemaEvolutionMode", "addNewColumns").option("cloudFiles.schemaLocation", "gs://what-a-bucket/FR5/").load("gs://what-a-bucket/FR6/")
display(df);


# COMMAND ----------

from pyspark.sql.functions import *

schema =    StructType([
    StructField("label",StringType(),True),
    StructField("concept",StringType(),True),
    StructField("unit",StringType(),True),
    StructField("value",StringType(),True)
])
df1 = df.select("cik", "symbol", explode("data").alias("X"))
df2 = df1.select("cik", "symbol", "X", explode("X.report.bs").alias("Y"))
df3 = df2.select("cik", "symbol", col("X.accessNumber"), col("X.year"), col("X.quarter"), col("X.form"), col("X.startDate"), col("X.endDate"), col("X.filedDate"), col("X.acceptedDate"), col("Y.label"), col("Y.concept"), col("Y.unit"), col("Y.value"))
display(df3)
df3.createOrReplaceTempView("records_2")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW records_v_2 AS 
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
# MAGIC     FROM records_2)
# MAGIC SELECT * FROM buff;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE  records_ml AS  (
# MAGIC SELECT * FROM records_v_2
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW total_assets_v AS 
# MAGIC   WITH CTE ( label_a, concept_a, symbol_a, year_a, unit_a, value_a, quarter_a, dup_a )
# MAGIC   AS (
# MAGIC     SELECT label as label_a, concept as concept_a, symbol as symbol_a, year as year_a, unit as unit_a, value as value_a, quarter as quarter_a, 
# MAGIC     ROW_NUMBER() OVER(PARTITION BY symbol, year, quarter
# MAGIC     ORDER BY label, concept) AS dup_a
# MAGIC     FROM records_2
# MAGIC     WHERE concept LIKE '%us-gaap_Assets' 
# MAGIC     )
# MAGIC   SELECT * FROM CTE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW total_liabilities_v AS 
# MAGIC   WITH CTE ( label_l, concept_l, symbol_l, year_l, unit_l, value_l, quarter_l, dup_l )
# MAGIC   AS (
# MAGIC     SELECT label as label_l, concept as concept_l, symbol as symbol_l, year as year_l, unit as unit_l, value as value_l, quarter as quarter_l, 
# MAGIC     ROW_NUMBER() OVER(PARTITION BY symbol, year, quarter
# MAGIC     ORDER BY label, concept) AS dup_l
# MAGIC     FROM records_2
# MAGIC     WHERE concept LIKE '%LiabilitiesAndStockholdersEquity'
# MAGIC     )
# MAGIC   SELECT * FROM CTE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW total_equities_v AS 
# MAGIC   WITH CTE ( label_e, concept_e, symbol_e, year_e, unit_e, value_e, quarter_e, dup_e )
# MAGIC   AS (
# MAGIC     SELECT label as label_e, concept as concept_e, symbol as symbol_e, year as year_e, unit as unit_e, value as value_e, quarter as quarter_e, 
# MAGIC     ROW_NUMBER() OVER(PARTITION BY symbol, year, quarter
# MAGIC     ORDER BY label, concept) AS dup_e
# MAGIC     FROM records_2
# MAGIC     WHERE concept LIKE 'StockholdersEquity' OR concept LIKE 'us-gaap_StockholdersEquity' 
# MAGIC     )
# MAGIC   SELECT * FROM CTE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW retained_earnings_v AS 
# MAGIC   WITH CTE ( label_r, concept_r, symbol_r, year_r, unit_r, value_r, quarter_r, dup_r )
# MAGIC   AS (
# MAGIC     SELECT label as label_r, concept as concept_r, symbol as symbol_r, year as year_r, unit as unit_r, value as value_r,  quarter as quarter_r, 
# MAGIC     ROW_NUMBER() OVER(PARTITION BY symbol, year, quarter
# MAGIC     ORDER BY label, concept) AS dup_r
# MAGIC     FROM records_2
# MAGIC     WHERE concept LIKE '%RetainedEarningsAccumulatedDeficit' 
# MAGIC     )
# MAGIC   SELECT * FROM CTE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW current_assets_v AS 
# MAGIC   WITH CTE ( label_ca, concept_ca, symbol_ca, year_ca, unit_ca, value_ca, quarter_ca, dup_ca )
# MAGIC   AS (
# MAGIC     SELECT label as label_ca, concept as concept_ca, symbol as symbol_ca, year as year_ca, unit as unit_ca, value as value_ca, quarter as quarter_ca, 
# MAGIC     ROW_NUMBER() OVER(PARTITION BY symbol, year, quarter
# MAGIC     ORDER BY label, concept) AS dup_ca
# MAGIC     FROM records_2
# MAGIC     WHERE concept LIKE 'us-gaap_AssetsCurrent'
# MAGIC     )
# MAGIC   SELECT * FROM CTE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW current_liabilities_v AS 
# MAGIC   WITH CTE ( label_cl, concept_cl, symbol_cl, year_cl, unit_cl, value_cl, quarter_cl, dup_cl )
# MAGIC   AS (
# MAGIC     SELECT label as label_cl, concept as concept_cl, symbol as symbol_cl, year as year_cl, unit as unit_cl, value as value_cl, quarter as quarter_cl, 
# MAGIC     ROW_NUMBER() OVER(PARTITION BY symbol, year, quarter
# MAGIC     ORDER BY label, concept) AS dup_cl
# MAGIC     FROM records_2
# MAGIC     WHERE concept LIKE 'us-gaap_LiabilitiesCurrent'
# MAGIC     )
# MAGIC   SELECT * FROM CTE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW net_inventories_v AS 
# MAGIC   WITH CTE ( label_i, concept_i, symbol_i, year_i, unit_i, value_i, quarter_i, dup_i )
# MAGIC   AS (
# MAGIC     SELECT label as label_i, concept as concept_i, symbol as symbol_i, year as year_i, unit as unit_i, value as value_i, quarter as quarter_i, 
# MAGIC     ROW_NUMBER() OVER(PARTITION BY symbol, year, quarter
# MAGIC     ORDER BY label, concept) AS dup_i
# MAGIC     FROM records_2
# MAGIC     WHERE concept LIKE 'us-gaap_InventoryNet' OR concept LIKE 'InventoryNet'
# MAGIC     )
# MAGIC   SELECT * FROM CTE

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import *
# MAGIC 
# MAGIC df_total_assets = spark.read.table('total_assets_v')
# MAGIC df_total_liabilities = spark.read.table('total_liabilities_v')
# MAGIC df_shareholder_equity = spark.read.table('total_equities_v')
# MAGIC df_retained_earnings = spark.read.table('retained_earnings_v')
# MAGIC df_current_assets = spark.read.table('current_assets_v')
# MAGIC df_current_liabilities = spark.read.table('current_liabilities_v')
# MAGIC df_net_inventories = spark.read.table('net_inventories_v')
# MAGIC 
# MAGIC # remove duplicate BS entities
# MAGIC 
# MAGIC df_total_assets = df_total_assets.filter("dup_a == 1")
# MAGIC df_total_liabilities = df_total_liabilities.filter("dup_l == 1")
# MAGIC df_shareholder_equity = df_shareholder_equity.filter("dup_e == 1")
# MAGIC df_retained_earnings = df_retained_earnings.filter("dup_r == 1")
# MAGIC df_current_assets = df_current_assets.filter("dup_ca == 1")
# MAGIC df_current_liabilities = df_current_liabilities.filter("dup_cl == 1")
# MAGIC df_net_inventories = df_net_inventories.filter("dup_i == 1")
# MAGIC 
# MAGIC #calculate working capital and current ratio
# MAGIC df_new = df_total_assets.join(df_total_liabilities, ( (df_total_assets["symbol_a"] == df_total_liabilities["symbol_l"]) & (df_total_assets["year_a"] == df_total_liabilities["year_l"]) & (df_total_assets["quarter_a"] == df_total_liabilities["quarter_l"]) & (df_total_assets["unit_a"] == df_total_liabilities["unit_l"]) ),"leftouter")
# MAGIC 
# MAGIC #clean up columns
# MAGIC drop_cols = ("label_a", "concept_a", "label_l", "concept_l", "symbol_l", "quarter_l", "year_l", "unit_l")
# MAGIC df_new = df_new.drop(*drop_cols)
# MAGIC df_new = df_new.withColumnRenamed("symbol_a","symbol").withColumnRenamed("year_a","year").withColumnRenamed("unit_a","unit").withColumnRenamed("value_a","total_assets").withColumnRenamed("quarter_a","quarter")
# MAGIC 
# MAGIC df_new = df_new.join(df_shareholder_equity, ( (df_new["symbol"] == df_shareholder_equity["symbol_e"]) & (df_new["year"] == df_shareholder_equity["year_e"]) & (df_new["quarter"] == df_shareholder_equity["quarter_e"]) & (df_new["unit"] == df_shareholder_equity["unit_e"]) ),"leftouter")
# MAGIC df_new = df_new.withColumn("total_liabilities", (col("value_l")-col("value_e")))
# MAGIC df_new = df_new.withColumn("debt_equity_ratio", (col("total_liabilities")/col("value_e")))
# MAGIC df_new = df_new.withColumn("debt_asset_ratio", (col("total_liabilities")/col("total_assets")))
# MAGIC 
# MAGIC df_new = df_new.join(df_retained_earnings, ( (df_new["symbol"] == df_retained_earnings["symbol_r"]) & (df_new["year"] == df_retained_earnings["year_r"]) & (df_new["quarter"] == df_retained_earnings["quarter_r"]) & (df_new["unit"] == df_retained_earnings["unit_r"]) ),"leftouter")
# MAGIC df_new = df_new.withColumn("return_on_assets", (col("value_r")/col("total_assets")))
# MAGIC df_new = df_new.withColumnRenamed("value_e","total_equities").withColumnRenamed("value_r","retained_earnings")
# MAGIC 
# MAGIC # current assets and liabilities
# MAGIC df_new = df_new.join(df_current_assets, ( (df_new["symbol"] == df_current_assets["symbol_ca"]) & (df_new["year"] == df_current_assets["year_ca"]) & (df_new["quarter"] == df_current_assets["quarter_ca"]) & (df_new["unit"] == df_current_assets["unit_ca"]) ),"leftouter")
# MAGIC 
# MAGIC df_new = df_new.join(df_current_liabilities, ( (df_new["symbol"] == df_current_liabilities["symbol_cl"]) & (df_new["year"] == df_current_liabilities["year_cl"]) & (df_new["quarter"] == df_current_liabilities["quarter_cl"]) & (df_new["unit"] == df_current_liabilities["unit_cl"]) ),"leftouter")
# MAGIC 
# MAGIC df_new = df_new.withColumn("working_capital", (col("value_ca")-col("value_cl")))
# MAGIC df_new = df_new.withColumn("current_ratio", (col("value_ca")/col("value_cl")))
# MAGIC df_new = df_new.withColumnRenamed("value_ca","current_assets").withColumnRenamed("value_cl","current_liabilities")
# MAGIC 
# MAGIC df_new = df_new.join(df_net_inventories, ( (df_new["symbol"] == df_net_inventories["symbol_i"]) & (df_new["year"] == df_net_inventories["year_i"]) & (df_new["quarter"] == df_net_inventories["quarter_i"]) & (df_new["unit"] == df_net_inventories["unit_i"]) ),"leftouter")
# MAGIC df_new = df_new.withColumnRenamed("value_i","net_inventories")
# MAGIC df_new = df_new.withColumn("quick_ratio", ((col("current_assets")-col("net_inventories"))/col("current_liabilities")))
# MAGIC 
# MAGIC #clean up columns
# MAGIC drop_cols = ("label_e", "concept_e", "label_r", "concept_r", "symbol_e", "quarter_e", "year_e", "unit_e", "symbol_r", "quarter_r", "year_r", "unit_r", "label_ca", "concept_ca", "label_cl", "concept_cl", "symbol_ca", "quarter_ca", "year_ca", "unit_ca", "symbol_cl", "quarter_cl", "year_cl", "unit_cl","quarter_i","symbol_i","label_i","concept_i","year_i","unit_i","value_l", "dup_a","dup_l","dup_e","dup_r","dup_ca","dup_cl","dup_i")
# MAGIC df_new = df_new.drop(*drop_cols)
# MAGIC 
# MAGIC 
# MAGIC display(df_new)
# MAGIC df_new.createOrReplaceTempView("records_3")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS SP100_1 (
# MAGIC   symbol STRING,
# MAGIC   year STRING,
# MAGIC   unit STRING,
# MAGIC   total_assets DOUBLE,
# MAGIC   quarter STRING,
# MAGIC   total_liabilities DOUBLE,
# MAGIC   debt_equity_ratio DOUBLE,
# MAGIC   debt_asset_ratio DOUBLE,
# MAGIC   retained_earnings DOUBLE,
# MAGIC   return_on_assets DOUBLE,
# MAGIC   current_assets DOUBLE,
# MAGIC   current_liabilities DOUBLE,
# MAGIC   working_capital DOUBLE,
# MAGIC   current_ratio DOUBLE,
# MAGIC   net_inventories DOUBLE,
# MAGIC   quick_ratio DOUBLE
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO SP100_1
# MAGIC USING records_3
# MAGIC ON SP100.year = records_3.year AND SP100.symbol = records_3.symbol AND SP100.quarter = records_3.quarter
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM SP100

# COMMAND ----------


