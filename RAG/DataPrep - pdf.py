# Databricks notebook source
pip install --upgrade pymupdf

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog prasanna;
# MAGIC use schema rag;
# MAGIC
# MAGIC list '/Volumes/prasanna/rag/pdfs'

# COMMAND ----------

import fitz
import glob

directory = glob.glob('/Volumes/prasanna/rag/pdfs/*.pdf')
df_data = [];
df_columns = ["id","title","content"]
index = 1;
for file_name in directory:
  doc = fitz.open(file_name) # open a document
  text = ''
  for page in doc: # iterate the document pages
    text = text + str(page.get_text().encode("utf8")) # get plain text (is in UTF-8)
  #print(text)
  df_data.append([index, file_name, text])
  index += 1
df = spark.createDataFrame(df_data, df_columns)
display(df)
#df.write.mode('overwrite').saveAsTable('pdfs_as_text')


# COMMAND ----------


