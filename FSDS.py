# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG financials_reported;
# MAGIC USE SCHEMA fsds2;

# COMMAND ----------

from pyspark.sql.functions import *

df = spark.read.option("header","true").option("delimiter","\t").option("inferSchema","true").csv("gs://what-a-bucket/sec-datasets/Financials-Statements/*/pre.txt")
#df = df.withColumn("ddate", to_date("ddate", "yyyyMMdd"))
df.write.saveAsTable("pre")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW total_assets_v AS (
# MAGIC   SELECT 
# MAGIC   num.adsh as adsh, cik, name,ein,form,period,fy,fp,stmt,uom,value as total_assets, ddate, 
# MAGIC   ROW_NUMBER() OVER(PARTITION BY num.adsh ORDER BY num.ddate) AS dup
# MAGIC   FROM sub,pre,num
# MAGIC   WHERE sub.adsh = pre.adsh AND sub.adsh = num.adsh AND pre.adsh = num.adsh AND pre.tag = num.tag 
# MAGIC   AND pre.version = num.version
# MAGIC   AND stmt = 'BS' AND form = '10-K' AND num.tag = 'Assets'
# MAGIC   ORDER BY name, num.tag
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW total_liabilities_v AS (
# MAGIC   SELECT 
# MAGIC   num.adsh as adsh_l,value as total_liabilities,
# MAGIC   ROW_NUMBER() OVER(PARTITION BY num.adsh ORDER BY num.ddate) AS dup
# MAGIC   FROM sub,pre,num
# MAGIC   WHERE sub.adsh = pre.adsh AND sub.adsh = num.adsh AND pre.adsh = num.adsh AND pre.tag = num.tag 
# MAGIC   AND pre.version = num.version
# MAGIC   AND stmt = 'BS' AND form = '10-K' AND num.tag = 'Liabilities'
# MAGIC   ORDER BY name, num.tag
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW inventory_net_v AS (
# MAGIC   SELECT 
# MAGIC   num.adsh as adsh, cik, name,ein,form,period,fy,fp,stmt,num.tag as tag,plabel,uom,value, ddate
# MAGIC   FROM sub,pre,num
# MAGIC   WHERE sub.adsh = pre.adsh AND sub.adsh = num.adsh AND pre.adsh = num.adsh AND pre.tag = num.tag 
# MAGIC   AND pre.version = num.version
# MAGIC   AND stmt = 'BS' AND form = '10-K' AND num.tag = 'InventoryNet'
# MAGIC   ORDER BY name, num.tag
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW current_assets_v AS (
# MAGIC   SELECT 
# MAGIC   num.adsh as adsh_ca,value as current_assets,
# MAGIC   ROW_NUMBER() OVER(PARTITION BY num.adsh ORDER BY num.ddate) AS dup
# MAGIC   FROM sub,pre,num
# MAGIC   WHERE sub.adsh = pre.adsh AND sub.adsh = num.adsh AND pre.adsh = num.adsh AND pre.tag = num.tag 
# MAGIC   AND pre.version = num.version
# MAGIC   AND stmt = 'BS' AND form = '10-K' AND num.tag = 'AssetsCurrent'
# MAGIC   ORDER BY name, num.tag
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW current_liabilities_v AS (
# MAGIC   SELECT 
# MAGIC   num.adsh as adsh_cl,value as current_liabilities,
# MAGIC   ROW_NUMBER() OVER(PARTITION BY num.adsh ORDER BY num.ddate) AS dup
# MAGIC   FROM sub,pre,num
# MAGIC   WHERE sub.adsh = pre.adsh AND sub.adsh = num.adsh AND pre.adsh = num.adsh AND pre.tag = num.tag 
# MAGIC   AND pre.version = num.version
# MAGIC   AND stmt = 'BS' AND form = '10-K' AND num.tag = 'LiabilitiesCurrent'
# MAGIC   ORDER BY name, num.tag
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW total_equities_v AS (
# MAGIC   SELECT 
# MAGIC   num.adsh as adsh_e,value as total_equities, 
# MAGIC   ROW_NUMBER() OVER(PARTITION BY num.adsh ORDER BY num.ddate) AS dup
# MAGIC   FROM sub,pre,num
# MAGIC   WHERE sub.adsh = pre.adsh AND sub.adsh = num.adsh AND pre.adsh = num.adsh AND pre.tag = num.tag 
# MAGIC   AND pre.version = num.version
# MAGIC   AND stmt = 'BS' AND form = '10-K' AND num.tag = 'StockholdersEquity'
# MAGIC   ORDER BY name, num.tag
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW cogs_v AS (
# MAGIC   SELECT 
# MAGIC   num.adsh as adsh_cogs,value as cogs, 
# MAGIC   ROW_NUMBER() OVER(PARTITION BY num.adsh ORDER BY num.ddate) AS dup
# MAGIC   FROM sub,pre,num
# MAGIC   WHERE sub.adsh = pre.adsh AND sub.adsh = num.adsh AND pre.adsh = num.adsh AND pre.tag = num.tag 
# MAGIC   AND pre.version = num.version
# MAGIC   AND stmt = 'IS' AND form = '10-K' AND num.tag = 'CostOfGoodsAndServicesSold'
# MAGIC   ORDER BY name, num.tag
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gross_profit_v AS (
# MAGIC   SELECT 
# MAGIC   num.adsh as adsh_gp,value as gross_profit, 
# MAGIC   ROW_NUMBER() OVER(PARTITION BY num.adsh ORDER BY num.ddate) AS dup
# MAGIC   FROM sub,pre,num
# MAGIC   WHERE sub.adsh = pre.adsh AND sub.adsh = num.adsh AND pre.adsh = num.adsh AND pre.tag = num.tag 
# MAGIC   AND pre.version = num.version
# MAGIC   AND stmt = 'IS' AND form = '10-K' AND num.tag = 'GrossProfit'
# MAGIC   ORDER BY name, num.tag
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW operating_expenses_v AS (
# MAGIC
# MAGIC   SELECT 
# MAGIC   num.adsh as adsh_opex,value as operating_expenses, 
# MAGIC   ROW_NUMBER() OVER(PARTITION BY num.adsh ORDER BY num.ddate) AS dup
# MAGIC   FROM sub,pre,num
# MAGIC   WHERE sub.adsh = pre.adsh AND sub.adsh = num.adsh AND pre.adsh = num.adsh AND pre.tag = num.tag 
# MAGIC   AND pre.version = num.version
# MAGIC   AND stmt = 'IS' AND form = '10-K' AND num.tag = 'OperatingExpenses'
# MAGIC   ORDER BY name, num.tag
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW profit_loss_v AS (
# MAGIC   SELECT 
# MAGIC   num.adsh as adsh_pl,value as profit_loss, 
# MAGIC   ROW_NUMBER() OVER(PARTITION BY num.adsh ORDER BY num.ddate) AS dup
# MAGIC   FROM sub,pre,num
# MAGIC   WHERE sub.adsh = pre.adsh AND sub.adsh = num.adsh AND pre.adsh = num.adsh AND pre.tag = num.tag 
# MAGIC   AND pre.version = num.version
# MAGIC   AND stmt = 'IS' AND form = '10-K' AND num.tag = 'ProfitLoss'
# MAGIC   ORDER BY name, num.tag
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW revenues_v AS (
# MAGIC   SELECT 
# MAGIC   num.adsh as adsh_rev,value as revenues, 
# MAGIC   ROW_NUMBER() OVER(PARTITION BY num.adsh ORDER BY num.ddate) AS dup
# MAGIC   FROM sub,pre,num
# MAGIC   WHERE sub.adsh = pre.adsh AND sub.adsh = num.adsh AND pre.adsh = num.adsh AND pre.tag = num.tag 
# MAGIC   AND pre.version = num.version
# MAGIC   AND stmt = 'IS' AND form = '10-K' AND num.tag = 'Revenues'
# MAGIC   ORDER BY name, num.tag
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT 
# MAGIC   num.adsh as adsh_rev,value as revenues, name, fy
# MAGIC   --ROW_NUMBER() OVER(PARTITION BY num.adsh ORDER BY num.ddate) AS dup
# MAGIC   FROM sub,pre,num
# MAGIC   WHERE 
# MAGIC    sub.adsh = pre.adsh AND sub.adsh = num.adsh AND pre.adsh = num.adsh AND pre.tag = num.tag 
# MAGIC   AND pre.version = num.version
# MAGIC   AND 
# MAGIC   stmt = 'IS' AND form = '10-K' AND num.tag = 'Revenues' AND name LIKE 'CISCO%'
# MAGIC   ORDER BY name, num.tag

# COMMAND ----------

from pyspark.sql.functions import *
 
df_total_assets = spark.read.table('total_assets_v')
df_total_liabilities = spark.read.table('total_liabilities_v')
df_total_equities = spark.read.table('total_equities_v')
df_current_assets = spark.read.table('current_assets_v')
df_current_liabilities = spark.read.table('current_liabilities_v')
df_cogs = spark.read.table('cogs_v')
df_revenues = spark.read.table('revenues_v')

df_total_assets = df_total_assets.filter("dup == 2")
df_total_liabilities = df_total_liabilities.filter("dup == 2")
df_total_equities = df_total_equities.filter("dup == 2")
df_current_assets = df_current_assets.filter("dup == 2")
df_current_liabilities = df_current_liabilities.filter("dup == 2")
df_cogs = df_cogs.filter("dup == 2")
df_revenues = df_revenues.filter("dup == 2")

df_new = df_total_assets.join(df_total_liabilities, ( (df_total_assets["adsh"] == df_total_liabilities["adsh_l"]) ) )
df_new = df_new.join(df_total_equities, ( (df_new["adsh"] == df_total_equities["adsh_e"]) ) )
df_new = df_new.join(df_current_assets, ( (df_new["adsh"] == df_current_assets["adsh_ca"]) ) )
df_new = df_new.join(df_current_liabilities, ( (df_new["adsh"] == df_current_liabilities["adsh_cl"]) ) )
df_new = df_new.join(df_cogs, ( (df_new["adsh"] == df_cogs["adsh_cogs"]) ))
df_new = df_new.join(df_revenues, ( (df_new["adsh"] == df_revenues["adsh_rev"]) ))

drop_cols = ("dup", "ddate", "adsh_l", "adsh_e", "adsh_ca", "adsh_cl","adsh_cogs","adsh_rev")
df_new = df_new.drop(*drop_cols)
df_new.createOrReplaceTempView("fin_stmts")

# COMMAND ----------


