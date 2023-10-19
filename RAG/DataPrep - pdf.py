# Databricks notebook source
!pip install --upgrade pymupdf
!pip install -U nltk

import nltk
nltk.download('punkt')

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog prasanna;
# MAGIC use schema rag;
# MAGIC
# MAGIC list '/Volumes/prasanna/rag/pdfs'

# COMMAND ----------

import fitz
import glob
import nltk
from nltk import tokenize
import re

directory = glob.glob('/Volumes/prasanna/rag/pdfs/*.pdf')
df_data = [];
df_columns = ["id","title","content"]
index = 1;
for file_name in directory:
  doc = fitz.open(file_name) # open a document
  text = ''
  for page in doc: # iterate the document pages
    text = text + str(page.get_text().encode("utf8")) # get plain text (is in UTF-8)
    text = text.replace('"', '')
    text = text.replace('\\n', ' ')
    text = text.replace('\\', '')
    text = re.sub("xc........|xe......."," ",text)
  sentences = tokenize.sent_tokenize(text)
  for sentence in sentences:
    df_data.append([index, file_name, sentence])
    index += 1
df = spark.createDataFrame(df_data, df_columns)
display(df)
df.write.mode('overwrite').saveAsTable('pdfs_as_text')


# COMMAND ----------


