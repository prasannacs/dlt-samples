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
# MAGIC SELECT 
# MAGIC pre.adsh,num.adsh, cik, name,ein,form,period,fy,fp,stmt,pre.tag,num.tag,plabel,uom,value, ddate
# MAGIC FROM sub,pre,num
# MAGIC WHERE sub.adsh = pre.adsh AND sub.adsh = num.adsh AND pre.adsh = num.adsh AND pre.tag = num.tag 
# MAGIC AND pre.version = num.version
# MAGIC --AND name LIKE 'CISCO%' 
# MAGIC AND stmt = 'IS' AND ddate > '2022-01-01' AND form = '10-K'
# MAGIC ORDER BY name, num.tag

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW total_assets_v AS (
# MAGIC   SELECT 
# MAGIC   num.adsh as adsh, cik, name,ein,form,period,fy,fp,stmt,num.tag as tag,plabel,uom,value, ddate
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
# MAGIC   num.adsh as adsh, cik, name,ein,form,period,fy,fp,stmt,num.tag as tag,plabel,uom,value, ddate
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
# MAGIC   num.adsh as adsh, cik, name,ein,form,period,fy,fp,stmt,num.tag as tag,plabel,uom,value, ddate
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
# MAGIC   num.adsh as adsh, cik, name,ein,form,period,fy,fp,stmt,num.tag as tag,plabel,uom,value, ddate
# MAGIC   FROM sub,pre,num
# MAGIC   WHERE sub.adsh = pre.adsh AND sub.adsh = num.adsh AND pre.adsh = num.adsh AND pre.tag = num.tag 
# MAGIC   AND pre.version = num.version
# MAGIC   AND stmt = 'BS' AND form = '10-K' AND num.tag = 'LiabilitiesCurrent'
# MAGIC   ORDER BY name, num.tag
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW equity_v AS (
# MAGIC   SELECT 
# MAGIC   num.adsh as adsh, cik, name,ein,form,period,fy,fp,stmt,num.tag as tag,plabel,uom,value, ddate
# MAGIC   FROM sub,pre,num
# MAGIC   WHERE sub.adsh = pre.adsh AND sub.adsh = num.adsh AND pre.adsh = num.adsh AND pre.tag = num.tag 
# MAGIC   AND pre.version = num.version
# MAGIC   AND stmt = 'BS' AND form = '10-K' AND num.tag = 'StockholdersEquity'
# MAGIC   ORDER BY name, num.tag
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW cogs_v AS (
# MAGIC   SELECT 
# MAGIC   num.adsh as adsh, cik, name,ein,form,period,fy,fp,stmt,num.tag as tag,plabel,uom,value, ddate
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
# MAGIC   num.adsh as adsh, cik, name,ein,form,period,fy,fp,stmt,num.tag as tag,plabel,uom,value, ddate
# MAGIC   FROM sub,pre,num
# MAGIC   WHERE sub.adsh = pre.adsh AND sub.adsh = num.adsh AND pre.adsh = num.adsh AND pre.tag = num.tag 
# MAGIC   AND pre.version = num.version
# MAGIC   AND stmt = 'IS' AND form = '10-K' AND num.tag = 'GrossProfit'
# MAGIC   ORDER BY name, num.tag
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW operating_expenses_v AS (
# MAGIC   SELECT 
# MAGIC   num.adsh as adsh, cik, name,ein,form,period,fy,fp,stmt,num.tag as tag,plabel,uom,value, ddate
# MAGIC   FROM sub,pre,num
# MAGIC   WHERE sub.adsh = pre.adsh AND sub.adsh = num.adsh AND pre.adsh = num.adsh AND pre.tag = num.tag 
# MAGIC   AND pre.version = num.version
# MAGIC   AND stmt = 'IS' AND form = '10-K' AND num.tag = 'OperatingExpenses'
# MAGIC   ORDER BY name, num.tag
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW profit_loss_v AS (
# MAGIC   SELECT 
# MAGIC   num.adsh as adsh, cik, name,ein,form,period,fy,fp,stmt,num.tag as tag,plabel,uom,value, ddate
# MAGIC   FROM sub,pre,num
# MAGIC   WHERE sub.adsh = pre.adsh AND sub.adsh = num.adsh AND pre.adsh = num.adsh AND pre.tag = num.tag 
# MAGIC   AND pre.version = num.version
# MAGIC   AND stmt = 'IS' AND form = '10-K' AND num.tag = 'ProfitLoss'
# MAGIC   ORDER BY name, num.tag
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW revenues_v AS (
# MAGIC   SELECT 
# MAGIC   num.adsh as adsh, cik, name,ein,form,period,fy,fp,stmt,num.tag as tag,plabel,uom,value, ddate
# MAGIC   FROM sub,pre,num
# MAGIC   WHERE sub.adsh = pre.adsh AND sub.adsh = num.adsh AND pre.adsh = num.adsh AND pre.tag = num.tag 
# MAGIC   AND pre.version = num.version
# MAGIC   AND stmt = 'IS' AND form = '10-K' AND num.tag = 'Revenues'
# MAGIC   ORDER BY name, num.tag
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from total_assets_v
# MAGIC where name like 'CISCO%'

# COMMAND ----------

from pyspark.sql.functions import *
 
df_total_assets = spark.read.table('total_assets_v')
df_total_liabilities = spark.read.table('total_liabilities_v')
df_shareholder_equity = spark.read.table('equity_v')

df_new = df_total_assets.join(df_total_liabilities, ( (df_total_assets["adsh"] == df_total_liabilities["adsh"]) & (df_total_assets["cik"] == df_total_liabilities["cik"]) & (df_total_assets["fy"] == df_total_liabilities["fy"]) & (df_total_assets["ddate"] == df_total_liabilities["ddate"]) ))

display(df_new)

# COMMAND ----------


