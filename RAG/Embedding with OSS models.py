# Databricks notebook source
# MAGIC %md # Example of registering and serving an OSS embedding model
# MAGIC
# MAGIC This notebook sets up the open-source text embedding model `e5-small-v2` in a Model Serving endpoint, usable for Vector Search.
# MAGIC * Download the model from the Hugging Face Hub.
# MAGIC * Register it to the MLflow Model Registry.
# MAGIC * Start a Model Serving endpoint with the model served there.
# MAGIC
# MAGIC Model `e5-small-v2` is published here: https://huggingface.co/intfloat/e5-small-v2
# MAGIC * MIT license
# MAGIC * with variants:
# MAGIC    * https://huggingface.co/intfloat/e5-large-v2
# MAGIC    * https://huggingface.co/intfloat/e5-base-v2
# MAGIC    * https://huggingface.co/intfloat/e5-small-v2
# MAGIC
# MAGIC This has been tested on DBR 13.2 ML, but may work with other versions.  See [DBR 13.2 ML release notes](https://docs.databricks.com/release-notes/runtime/13.2ml.html) for specific library versions.

# COMMAND ----------

# MAGIC %md ## Download model

# COMMAND ----------

# Download model using the sentence_transformers library.
from sentence_transformers import SentenceTransformer

source_model_name = 'intfloat/e5-small-v2'  # model name on Hugging Face Hub
model = SentenceTransformer(source_model_name)

# COMMAND ----------

# Test the model, just to show it works.
sentences = ["This is an example sentence", "Each sentence is converted."]
embeddings = model.encode(sentences)
print(embeddings)

# COMMAND ----------

# MAGIC %md ## Register model to MLflow

# COMMAND ----------

# MLflow model name: The Model Registry will use this name for the model.
registered_model_name = 'e5-small-v2'

# COMMAND ----------

import mlflow

# COMMAND ----------

# Compute input/output schema.
signature = mlflow.models.signature.infer_signature(sentences, embeddings)
print(signature)

# COMMAND ----------

model_info = mlflow.sentence_transformers.log_model(
  model,
  artifact_path="model",
  signature=signature,
  input_example=sentences,
  registered_model_name=registered_model_name)

# COMMAND ----------

# This programmatically extracts the version of the model you just registered.
# In practice, you might promote models to stage "Production" before using them.
mlflow_client = mlflow.MlflowClient()
models = mlflow_client.get_latest_versions(registered_model_name, stages=["None"])
model_version = models[0].version
model_version

# COMMAND ----------

# MAGIC %md ## Start Model Serving endpoint
# MAGIC
# MAGIC See the [Model Serving docs](https://docs.databricks.com/machine-learning/model-serving/index.html) for more details.

# COMMAND ----------

endpoint_name = "e5-small-v2"  # Name of endpoint to create

# Workspace URL for REST API call
workspace_url = "https://" + spark.conf.get("spark.databricks.workspaceUrl")

# Get current user's token for API call.
# This is a hack, and it is better to create endpoints using a token created for a Service Principal.
# See https://docs.databricks.com/dev-tools/service-principals.html
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# COMMAND ----------

import json
import pandas as pd
import requests
import time

# COMMAND ----------

# MAGIC %md ### Create endpoint
# MAGIC
# MAGIC See the [Create and manage model serving endpoints](https://docs.databricks.com/machine-learning/model-serving/create-manage-serving-endpoints.html) docs for more details.

# COMMAND ----------

deploy_headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
deploy_url = f'{workspace_url}/api/2.0/serving-endpoints'
endpoint_config = {
  "name": endpoint_name,
  "config": {
    "served_models": [{
      "name": f'{registered_model_name.replace(".", "_")}_{1}',
      "model_name": registered_model_name,
      "model_version": model_version,
      "workload_type": "CPU",
      "workload_size": "Small",
      "scale_to_zero_enabled": True,
    }]
  }
}

endpoint_json = json.dumps(endpoint_config, indent='  ')
deploy_response = requests.request(method='POST', headers=deploy_headers, url=deploy_url, data=endpoint_json)
if deploy_response.status_code != 200:
  raise Exception(f'Request failed with status {deploy_response.status_code}, {deploy_response.text}')

print(deploy_response.json())

# COMMAND ----------

# MAGIC %md ### Query endpoint (once ready)
# MAGIC
# MAGIC Before testing the endpoint below, please check the Databricks UI for the serving endpoint readiness.
# MAGIC The endpoint may take several minutes to get ready.
# MAGIC
# MAGIC See the [Send scoring requests to serving endpoints](https://docs.databricks.com/machine-learning/model-serving/score-model-serving-endpoints.html) docs for more details.

# COMMAND ----------

# Prepare data for query.
sentences = ['Hello world', 'Good morning']
ds_dict = {'dataframe_split': pd.DataFrame(pd.Series(sentences)).to_dict(orient='split')}
data_json = json.dumps(ds_dict, allow_nan=True)
print(data_json)

# COMMAND ----------

invoke_headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
invoke_url = f'{workspace_url}/serving-endpoints/{endpoint_name}/invocations'
print(invoke_url)

start = time.time()
invoke_response = requests.request(method='POST', headers=invoke_headers, url=invoke_url, data=data_json, timeout=360)
end = time.time()
print(f'time in seconds: {end-start}')

if invoke_response.status_code != 200:
  raise Exception(f'Request failed with status {invoke_response.status_code}, {invoke_response.text}')

print(invoke_response.text)

# COMMAND ----------


