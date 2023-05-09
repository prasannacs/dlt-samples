# Databricks notebook source
# MAGIC %pip install farm-haystack[colab,ocr,preprocessing,file-conversion,pdf,faiss]

# COMMAND ----------

# MAGIC %sql
# MAGIC CLEAR CACHE

# COMMAND ----------

from haystack.utils import convert_files_to_docs, clean_wiki_text
from haystack.nodes import PreProcessor
from haystack.nodes import PDFToTextConverter
from haystack.utils import fetch_archive_from_http
from pathlib import Path


# This fetches some sample files to work with
doc_dir = "/dbfs/local_disk0/tmp/test1"
#s3_url = "https://s3.eu-central-1.amazonaws.com/deepset.ai-farm-qa/datasets/documents/preprocessing_tutorial8.zip"
s3_url= "https://the-bucket-3345345.s3.amazonaws.com/reports.zip"
fetch_archive_from_http(url=s3_url, output_dir=doc_dir)


converter = PDFToTextConverter(remove_numeric_tables=True, valid_languages=["en"])

docs = converter.convert(file_path="/dbfs/local_disk0/tmp/test1/citi-2022-annual-report.pdf", meta=None)

#docs = convert_files_to_docs(dir_path="dbfs:/FileStore/shared_uploads/prasanna.selvaraj user1@databricks.com/", clean_func=clean_wiki_text, split_paragraphs=True)
preprocessor = PreProcessor(
 clean_empty_lines=True,
 clean_whitespace=True,
 clean_header_footer=False,
 split_by="word",
 split_length=100,
 split_overlap=3,
 split_respect_sentence_boundary=False,
)

processed_docs = preprocessor.process(docs)
print(processed_docs)

# COMMAND ----------

from haystack.document_stores import FAISSDocumentStore

document_store = FAISSDocumentStore(faiss_index_factory_str="Flat", embedding_dim=1536)
document_store.delete_documents()
document_store.write_documents(processed_docs)

# COMMAND ----------

from haystack.nodes import EmbeddingRetriever

retriever = EmbeddingRetriever(
 document_store=document_store,
 embedding_model="text-embedding-ada-002",
 batch_size = 32,
 api_key="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",
 max_seq_len = 1024
)

# COMMAND ----------

document_store.update_embeddings(retriever)

# COMMAND ----------

from haystack.nodes import OpenAIAnswerGenerator

generator = OpenAIAnswerGenerator(api_key="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", model="text-davinci-003", temperature=.5, max_tokens=30)

# COMMAND ----------

from haystack.pipelines import GenerativeQAPipeline

gpt_search_engine = GenerativeQAPipeline(generator=generator, retriever=retriever)

# COMMAND ----------



# COMMAND ----------

from haystack.utils import print_answers

query = "what is citi's net income?"
params = {"Retriever": {"top_k": 5}, "Generator": {"top_k": 1}}

answer = gpt_search_engine.run(query=query, params=params)
print_answers(answer, details="minimum")


# COMMAND ----------


