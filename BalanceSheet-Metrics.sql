-- Databricks notebook source
-- MAGIC %sql
-- MAGIC USE CATALOG Financials;
-- MAGIC USE SCHEMA balanceSheet;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW current_assets_v AS (
SELECT label as label_a, concept as concept_a, symbol as symbol_a, year as year_a, unit as unit_a, value as value_a, quarter as quarter_a FROM records3 where label = 'Total current assets')

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW current_liabilities_v AS (
SELECT label as label_l, concept as concept_l, symbol as symbol_l, year as year_l, unit as unit_l, value as value_l, quarter as quarter_l FROM records3 where label = 'Total current liabilities')

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW net_inventories_v AS (
  SELECT label as label_i, concept as concept_i, symbol as symbol_i, year as year_i, unit as unit_i, value as value_i, quarter as quarter_i FROM records3 where concept = 'InventoryNet'
)

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW shareholders_equity_v AS (
  SELECT label as label_she, concept as concept_she, symbol as symbol_she, year as year_she, unit as unit_she, value as value_she, quarter as quarter_she FROM records3 where concept = "StockholdersEquity"
)

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW total_assets_v AS (
SELECT label as label_a, concept as concept_a, symbol as symbol_a, year as year_a, unit as unit_a, value as value_a, quarter as quarter_a FROM records3 where label = 'Total assets')

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW total_liabilities_v AS (
SELECT label as label_l, concept as concept_l, symbol as symbol_l, year as year_l, unit as unit_l, value as value_l, quarter as quarter_l FROM records3 where label = 'Total liabilities')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC 
-- MAGIC df_current_assets = spark.read.table('current_assets_v')
-- MAGIC df_current_liabilities = spark.read.table('current_liabilities_v')
-- MAGIC df_net_inventories = spark.read.table('net_inventories_v')
-- MAGIC df_total_assets = spark.read.table('total_assets_v')
-- MAGIC df_total_liabilities = spark.read.table('total_liabilities_v')
-- MAGIC df_shareholder_equity = spark.read.table('shareholders_equity_v')
-- MAGIC 
-- MAGIC #calculate working capital and current ratio
-- MAGIC df_new = df_current_assets.join(df_current_liabilities, ( (df_current_assets["symbol_a"] == df_current_liabilities["symbol_l"]) & (df_current_assets["year_a"] == df_current_liabilities["year_l"]) & (df_current_assets["quarter_a"] == df_current_liabilities["quarter_l"]) & (df_current_assets["unit_a"] == df_current_liabilities["unit_l"]) ))
-- MAGIC 
-- MAGIC df_new = df_new.withColumn("working_capital", col("value_a")-col("value_l"))
-- MAGIC df_new = df_new.withColumn("current_ratio", col("value_a")/col("value_l"))
-- MAGIC 
-- MAGIC #clean up columns
-- MAGIC drop_cols = ("label_a", "concept_a", "label_l", "concept_l", "symbol_l", "quarter_l", "year_l", "unit_l")
-- MAGIC df_new = df_new.drop(*drop_cols)
-- MAGIC df_new = df_new.withColumnRenamed("symbol_a","symbol").withColumnRenamed("year_a","year").withColumnRenamed("unit_a","unit").withColumnRenamed("value_a","total_current_assets").withColumnRenamed("value_l","total_current_liabilities").withColumnRenamed("quarter_a","quarter")
-- MAGIC 
-- MAGIC # calcualte quick ratio
-- MAGIC df_new = df_new.join(df_net_inventories, ( (df_new["symbol"] == df_net_inventories["symbol_i"]) & (df_new["year"] == df_net_inventories["year_i"]) & (df_new["quarter"] == df_net_inventories["quarter_i"]) & (df_new["unit"] == df_net_inventories["unit_i"]) ))
-- MAGIC 
-- MAGIC df_new = df_new.withColumn("quick_ratio", ((col("total_current_assets")-col("value_i"))/col("total_current_liabilities")))
-- MAGIC 
-- MAGIC # calculate debt-to-asset ratio
-- MAGIC #df_new = df_new.join(df_total_assets, ( (df_new["symbol"] == df_total_assets["symbol_a"]) & (df_new["year"] == df_total_assets["year_a"]) & (df_new["quarter"] == df_total_assets["quarter_a"]) & (df_new["unit"] == df_total_assets["unit_a"]) ))
-- MAGIC #df_new = df_new.join(df_total_liabilities, ( (df_new["symbol"] == df_total_liabilities["symbol_l"]) & (df_new["year"] == df_total_liabilities["year_l"]) & (df_new["quarter"] == df_total_liabilities["quarter_l"]) & (df_new["unit"] == df_total_liabilities["unit_l"]) ))
-- MAGIC 
-- MAGIC #df_new = df_new.withColumn("debt_asset_ratio", (col("value_l")/col("value_a")))
-- MAGIC 
-- MAGIC #clean up columns
-- MAGIC drop_cols = ("label_i", "concept_i", "symbol_i", "quarter_i", "year_i", "unit_i", "label_a", "concept_a", "symbol_a", "quarter_a", "year_a", "unit_a", "label_l", "concept_l", "symbol_l", "quarter_l", "year_l", "unit_l")
-- MAGIC df_new = df_new.drop(*drop_cols)
-- MAGIC df_new = df_new.withColumnRenamed("value_i","net_inventories")
-- MAGIC 
-- MAGIC display(df_new)
-- MAGIC display(df_total_assets.count())

-- COMMAND ----------


