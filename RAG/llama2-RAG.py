# Databricks notebook source
# MAGIC %pip install --upgrade "mlflow-skinny[databricks]>=2.4.1"
# MAGIC %pip install --upgrade --force-reinstall databricks-vectorsearch
# MAGIC %pip install mlflow[databricks]
# MAGIC dbutils.library.restartPython()
# MAGIC

# COMMAND ----------

from huggingface_hub import notebook_login

# Login to Huggingface to get access to the model
notebook_login()

# COMMAND ----------

model = "meta-llama/Llama-2-7b-chat-hf"
revision = "08751db2aca9bf2f7f80d2e516117a53d7450235"

from huggingface_hub import snapshot_download

# If the model has been downloaded in previous cells, this will not repetitively download large model files, but only the remaining files in the repo
snapshot_location = snapshot_download(repo_id=model, revision=revision, ignore_patterns="*.safetensors*")

# COMMAND ----------

import mlflow
import torch
import transformers
from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient()
# Define prompt template to get the expected features and performance for the chat versions. See our reference code in github for details: https://github.com/facebookresearch/llama/blob/main/llama/generation.py#L212

DEFAULT_SYSTEM_PROMPT = """\
You are a helpful, respectful and honest assistant. Always answer as helpfully as possible, while being safe. Your answers should not include any harmful, unethical, racist, sexist, toxic, dangerous, or illegal content. Please ensure that your responses are socially unbiased and positive in nature.

If a question does not make any sense, or is not factually coherent, explain why instead of answering something not correct. If you don't know the answer to a question, please don't share false information."""

# Define PythonModel to log with mlflow.pyfunc.log_model

class Llama2(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        """
        This method initializes the tokenizer and language model
        using the specified model repository.
        """
        # Initialize tokenizer and language model
        self.tokenizer = transformers.AutoTokenizer.from_pretrained(
            context.artifacts['repository'], padding_side="left")
        self.model = transformers.AutoModelForCausalLM.from_pretrained(
            context.artifacts['repository'], 
            torch_dtype=torch.bfloat16,
            low_cpu_mem_usage=True, 
            trust_remote_code=True,
            device_map="auto",
            pad_token_id=self.tokenizer.eos_token_id)
        self.model.eval()

    def _build_prompt(self, instruction):
        """
        This method generates the prompt for the model.
        """
        return f"""<s>[INST]<<SYS>>\n{DEFAULT_SYSTEM_PROMPT}\n<</SYS>>\n\n\n{instruction}[/INST]\n"""

    def _generate_response(self, prompt, temperature, max_new_tokens):
        """
        This method generates prediction for a single input.
        """
        # Build the prompt
        prompt = self._build_prompt(prompt)

        # Encode the input and generate prediction
        encoded_input = self.tokenizer.encode(prompt, return_tensors='pt').to('cuda')
        output = self.model.generate(encoded_input, do_sample=True, temperature=temperature, max_new_tokens=max_new_tokens)
    
        # Decode the prediction to text
        generated_text = self.tokenizer.decode(output[0], skip_special_tokens=True)

        # Removing the prompt from the generated text
        prompt_length = len(self.tokenizer.encode(prompt, return_tensors='pt')[0])
        generated_response = self.tokenizer.decode(output[0][prompt_length:], skip_special_tokens=True)

        return generated_response
      
    def _semanticSearch(self, prompt):
      vs_index_fullname = "prasanna.rag.pdf_index2"
      vector_search_endpoint_name = "adp"
      index = vsc.get_index(vs_index_fullname,vector_search_endpoint_name)
      query_resp = index.similarity_search(query_text=prompt, columns=["id", "title", "content"],#filters={"title": "/Volumes/prasanna/rag/pdfs/P&g.pdf"}
                                           )
      result = query_resp["result"].get("data_array")[0][2]
      return result
      
    def predict(self, context, model_input):
        """
        This method generates prediction for the given input.
        """

        outputs = []

        for i in range(len(model_input)):
          prompt = model_input["prompt"][i]
          prompt = prompt + ' Adding additional context: ' + self._semanticSearch(prompt) 
          print(prompt)
          temperature = model_input.get("temperature", [1.0])[i]
          max_new_tokens = model_input.get("max_new_tokens", [100])[i]

          outputs.append(self._generate_response(prompt, temperature, max_new_tokens))
      
        # {"candidates": [...]} is the required response format for MLflow AI gateway -- see 07_ai_gateway for example
        return {"candidates": outputs}

# COMMAND ----------

from mlflow.models.signature import ModelSignature
from mlflow.types import DataType, Schema, ColSpec

import pandas as pd

# Define input and output schema
input_schema = Schema([
    ColSpec(DataType.string, "prompt"), 
    ColSpec(DataType.double, "temperature", optional=True), 
    ColSpec(DataType.long, "max_new_tokens", optional=True)])
output_schema = Schema([ColSpec(DataType.string)])
signature = ModelSignature(inputs=input_schema, outputs=output_schema)

# Define input example
input_example=pd.DataFrame({
            "prompt":["what is Tesla's Vision?"], 
            "temperature": [0.5],
            "max_new_tokens": [100]})

# Log the model with its details such as artifacts, pip requirements and input example
# This may take about 1.7 minutes to complete
mlflow.set_experiment("/Users/prasanna.selvaraj@databricks.com/llamav2-rag")
with mlflow.start_run() as run:  
    mlflow.pyfunc.log_model(
        "model",
        python_model=Llama2(),
        artifacts={'repository' : snapshot_location},
        pip_requirements=["torch", "transformers", "accelerate", "sentencepiece"],
        input_example=input_example,
        signature=signature,
    )

# COMMAND ----------

import mlflow
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# Register model to Unity Catalog
# This may take 2.2 minutes to complete

registered_name = "prasanna.rag.llama2-rag" # Note that the UC model name follows the pattern <catalog_name>.<schema_name>.<model_name>, corresponding to the catalog, schema, and registered model name


result = mlflow.register_model(
    "runs:/"+run.info.run_id+"/model",
    registered_name,
)

# COMMAND ----------

import mlflow
import pandas as pd

loaded_model = mlflow.pyfunc.load_model(f"models:/{registered_name}/10")

# Make a prediction using the loaded model
loaded_model.predict(
    {
        "prompt": ["what is Teslas vision?"],
        "temperature": [0.5],
        "max_new_tokens": [100],
    }
)

# COMMAND ----------


