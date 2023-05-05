# Databricks notebook source
# MAGIC %pip install accelerate
# MAGIC %pip install transformers[torch]==4.25.1

# COMMAND ----------

import torch
from transformers import pipeline

generate_text = pipeline(model="databricks/dolly-v2-7b", torch_dtype=torch.bfloat16, trust_remote_code=True, device_map="auto")


# COMMAND ----------

generate_text("Explain to me the difference between nuclear fission and fusion.")


# COMMAND ----------


