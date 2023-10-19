# Databricks notebook source
# MAGIC %md # Vector Search Python SDK example usage
# MAGIC
# MAGIC This notebook demonstrates usage of the Vector Search Python SDK, which provides a `VectorSearchClient` as a primary API for working with Vector Search.
# MAGIC
# MAGIC Alternatively, you may call the REST API directly.
# MAGIC
# MAGIC **Pre-req**: This notebook assumes you have already created a Model Serving endpoint for the embedding model.  See `embedding_model_endpoint` below, and the companion notebook for creating endpoints.

# COMMAND ----------

# MAGIC %pip install --upgrade --force-reinstall databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient()
help(VectorSearchClient)

# COMMAND ----------

# MAGIC %md ## Load toy dataset into source Delta table

# COMMAND ----------

# We will create the following source Delta table.
source_catalog = "prasanna"
source_schema = "rag"
source_table = "pdfs_as_text"
source_table_fullname = f"{source_catalog}.{source_schema}.{source_table}"
display(spark.sql(f"SELECT * FROM {source_table_fullname}"))

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE prasanna.rag.pdfs_as_text SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create Vector Search Endpoint

# COMMAND ----------

#vector_search_endpoint_name = "vector-search-demo-endpoint"
vector_search_endpoint_name = "adp"

# COMMAND ----------

vsc.create_endpoint(
    name=vector_search_endpoint_name,
    endpoint_type="STANDARD"
)

# COMMAND ----------

vsc.get_endpoint(
  name=vector_search_endpoint_name
)
vsc.list_endpoints()


# COMMAND ----------

# MAGIC %md ## Create vector index

# COMMAND ----------

# Vector index
vs_index = "pdf_index2"
vs_index_fullname = f"{source_catalog}.{source_schema}.{vs_index}"

embedding_model_endpoint = "jingos-bge-large-en"

# COMMAND ----------

vsc.create_delta_sync_index(
  endpoint_name="adp",
  source_table_name=source_table_fullname,
  index_name=vs_index_fullname,
  pipeline_type='CONTINUOUS',
  primary_key="id",
  embedding_source_column="content",
  embedding_model_endpoint_name=embedding_model_endpoint
)

# COMMAND ----------

index = vsc.get_index(vs_index_fullname,vector_search_endpoint_name)
#index.describe(detailed_telemetry=True)
index.describe()

# COMMAND ----------

# MAGIC %md ## Similarity search
# MAGIC
# MAGIC Query the Vector Index to find similar documents!

# COMMAND ----------

print(vs_index_fullname)

index = vsc.get_index(vs_index_fullname,vector_search_endpoint_name)
query_resp = index.similarity_search(
query_text="P&G vision",
columns=["id", "title", "content"],
filters={"title": "/Volumes/prasanna/rag/pdfs/ADP Databricks Serverless.pdf"}
)

print(query_resp)

# COMMAND ----------

# MAGIC %md ## Delete vector index

# COMMAND ----------

vsc.delete_index(index_name=vs_index_fullname)

# COMMAND ----------


