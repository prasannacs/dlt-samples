# Databricks notebook source
# MAGIC %pip install beautifulsoup4 tiktoken 
# MAGIC %pip install transformers==4.30.2 "unstructured[pdf,docx]==0.10.30" langchain==0.1.5 llama-index==0.9.3 databricks-vectorsearch==0.22 pydantic==1.10.9 mlflow==2.10.1
# MAGIC %pip install langchain-ai21
# MAGIC %pip install transformers -U
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `prasanna`;
# MAGIC use schema `crawler_3`;
# MAGIC -- please create a new schema and don't overwrite any tables - i have a demo with ADP CDO on Tuesday
# MAGIC

# COMMAND ----------

import requests
import pandas as pd
import concurrent.futures
from bs4 import BeautifulSoup
import re
from pyspark.sql.types import StringType

# Function to fetch HTML content for a given URL
def fetch_html(url):
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.content
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

# Function to process a URL and extract text from the specified div
def process_url(url):
    html_content = fetch_html(url)
    if html_content:
        soup = BeautifulSoup(html_content, "html.parser")
        for a in soup.find_all('a'):
          a.decompose()
          # Get the text content
        text_content = soup.get_text()
        #print(text_content)
        title = soup.find("title")
        text_content = text_content.replace("\n"," ")
        return text_content
    return None
spark.udf.register("process_url_udf", process_url, StringType())
process_url_udf = udf(process_url, StringType())


# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, col
from pyspark.sql.types import StructType, StructField, StringType

df1 = spark.read.option("delimiter","|").csv("dbfs:/Volumes/prasanna/crawler_1/source/philly.csv", header=True, inferSchema=True)
df1 = df1.withColumn("id", monotonically_increasing_id())
df1 = df1.withColumn("parent_url_content", process_url_udf("parent_url"))
df1.write.saveAsTable('raw_philly_v1')

# COMMAND ----------

# download all PDFs to volumes

from pyspark.sql.functions import col, endswith
import requests

HEADERS = {'User-Agent': 'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148'}

df = spark.sql("SELECT * FROM raw_philly_v1")
df1 = df.filter(col("doc_url").contains(".pdf"))
rows = df1.collect()
for row in rows:
  response = requests.get(row.doc_url, headers=HEADERS)
  if response.status_code == 200:
    file_path = "/Volumes/prasanna/crawler_3/pdfs/"+str(row.id)+'.pdf'
    with open(file_path, 'wb') as f:
      f.write(response.content)
  print('PDF downloaded successfully!')

# COMMAND ----------

#install poppler on the cluster (should be done by init scripts)
def install_ocr_on_nodes():
    """
    install poppler on the cluster (should be done by init scripts)
    """
    # from pyspark.sql import SparkSession
    import subprocess
    num_workers = max(1,int(spark.conf.get("spark.databricks.clusterUsageTags.clusterWorkers")))
    command = "sudo rm -rf /var/cache/apt/archives/* /var/lib/apt/lists/* && sudo apt-get clean && sudo apt-get update && sudo apt-get install poppler-utils tesseract-ocr -y" 
    subprocess.check_output(command, shell=True)

    def run_command(iterator):
        for x in iterator:
            yield subprocess.check_output(command, shell=True)

    # spark = SparkSession.builder.getOrCreate()
    data = spark.sparkContext.parallelize(range(num_workers), num_workers) 
    # Use mapPartitions to run command in each partition (worker)
    output = data.mapPartitions(run_command)
    try:
        output.collect();
        print("OCR libraries installed")
    except Exception as e:
        print(f"Couldn't install on all node: {e}")
        raise e

install_ocr_on_nodes()

# COMMAND ----------

from unstructured.partition.auto import partition
from unstructured.partition.pdf import partition_pdf
from unstructured.chunking.title import chunk_by_title
from pyspark.sql.types import StructType, StructField, StringType
import glob
import re
import requests
import io
import re

def find_table_title(html_content):
  splits = re.split("<th.*?>(.+?)</th>",html_content)
  fileName = splits[len(splits)-1]
  year = re.findall("[2][0-9][0-9][0-4]",fileName)
  return {'file_name':fileName, 'year':year}

directory = glob.glob('/Volumes/prasanna/crawler_3/pdfs/*.pdf')
data = []
for file_name in directory:
  print(file_name)
  sections = partition_pdf(file_name, strategy="hi_res", hi_res_model_name="detectron2_onnx", infer_table_structure=True)
  section_title = []
  table_title = ''
  text_content = ''
  for section in sections:
    if( section.category == 'Title'):
      section_title.append(section.text)
      table_title = section.text
    if( section.category == 'Table'):
      if( section.metadata is not None):
        if( section.metadata.text_as_html is not None):  
          delimiter = ", "
          table_content = '-- Document title '+delimiter.join(section_title)+' -- \n'
          table_content += '-- Table title '+table_title+' -- \n'
          table_content += section.metadata.text_as_html
          data.append({"file_name": file_name, "pdf_content": table_content})
    if( section.category != 'Table'):
      text_content += section.text
  data.append({"file_name": file_name, "pdf_content": text_content})

  # Default split is by section of document, concatenate them all together because we want to split by sentence instead.
  #data.append({"file_name": file_name, "pdf_content": pdf_txt})
df = spark.createDataFrame(data)
display(df)
df.write.mode("overwrite").saveAsTable("pdf_chunks_v1")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE raw_philly_docs SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
# MAGIC
# MAGIC ALTER TABLE raw_philly_docs 
# MAGIC DROP COLUMN pdf_bytes;

# COMMAND ----------

from pyspark.sql.functions import col, substring

df1 = spark.read.table("pdf_chunks_v1")
df1 = df1.withColumn('pdf_id',substring('file_name',34,2))
#display(df1)
df2 = spark.read.table("raw_philly_v1")
df2 = df2.drop("pdf_content")
df2 = df2.drop("pdf_bytes")
df2 = df2.join(df1, df2["id"] == df1["pdf_id"], "leftouter")
display(df2)
df2.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("philly_docs_v1")




# COMMAND ----------

from unstructured.partition.html import partition_html

HEADERS = {'User-Agent': 'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148'}
df1 = spark.read.table("raw_philly_v1")
df2 = df1.select("parent_url", "id")
data = []
for url in df2.collect():
  html_text = ''
  print(url.parent_url, url.id)
  elements = partition_html(url=url.parent_url, headers=HEADERS)
  for element in elements:
    if( element.metadata is not None):
      if( element.metadata.text_as_html is not None):
        #print(element.metadata.text_as_html)
        html_text += element.metadata.text_as_html
      else:
        #print(element.text)
        html_text += element.text
  data.append({"parent_url":url.parent_url, "html_text":html_text, "id": url.id })
df3 = spark.createDataFrame(data)
display(df3)
df3.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("philly_html_v1")



# COMMAND ----------


# the below snippet joins the pdf contents to the main table

from pyspark.sql.functions import concat_ws, col, lit

df1 = spark.read.table("philly_docs_v1")
df2 = spark.read.table("philly_html_v1")
df2 = df2.drop("parent_url")
df2 = df2.withColumnRenamed("id", "html_id")
df3 = df2.join(df1, df2["html_id"] == df1["id"], "inner")

df3 = df3.withColumn("html_pdf_content", concat_ws("----",col("html_text"),col("pdf_content")))
display(df3)
# Please create a new table
df3.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("philly_html_pdf_v1")


# COMMAND ----------

# code not required anymore

import os
from getpass import getpass

os.environ["AI21_API_KEY"] = getpass()

# COMMAND ----------

# code not required anymore

from langchain_ai21 import AI21SemanticTextSplitter
from langchain_core.documents import Document

TEXT = (
    "CITY OF PHILADELPHIA DEPARTMENT OF REVENUE November 5, 2020 (REVISED) Wage Tax policy guidance for non-resident employees Non-resident employees who work for Philadelphia-based employers are not subject to Philadelphia Wage Tax during the time they are required to work outside of Philadelphia. The Philadelphia Department of Revenue has not changed its Wage Tax policy during the COVID-19 pandemic. Schedules to withhold and remit the tax to the City remain the same. We are publishing this guidance at a time when employees have been forced to perform their duties from home, many for the first time. This is the policy: The City of Philadelphia uses a “requirement of employment” standard that applies to all non-residents whose base of operation is the employer’s location within Philadelphia. Under this standard, a non- resident employee is not subject to the Wage Tax when the employer requires him or her to perform a job outside of Philadelphia including working from home. A non-resident employee who works from home for his or her convenience is not exempt from the Wage Tax – even with his or her employer’s authorization. On the other hand, if a Philadelphia employer requires a non-resident to perform duties outside the city, he or she is exempt from the Wage Tax for the days spent fulfilling that work. Non-resident employees who had Wage Tax withheld during the time they were required to perform their duties from home in 2020, may file for a refund with a Wage Tax refund petition in 2021. Employees file for a refund after the end of the tax year and will need to provide a copy of their W-2 form. Employees must provide a letter from their employer stating they were required to work from home. The letter, which must be on company letterhead and signed, is submitted along with the refund petition. The City requires an employer to withhold and remit Wage Tax for all its Philadelphia residents, regardless of where they perform their duties.CITY OF PHILADELPHIA DEPARTMENT OF REVENUENovember 5, 2020 (REVISED)Wage Tax policy guidance for non-resident employeesNon-resident employees who work for Philadelphia-based employers are not subject to Philadelphia Wage Tax during the time they are required to work outside of Philadelphia.The Philadelphia Department of Revenue has not changed its Wage Tax policy during the COVID-19 pandemic. Schedules to withhold and remit the tax to the City remain the same. We are publishing this guidance at a time when employees have been forced to perform their duties from home, many for the first time.This is the policy:The City of Philadelphia uses a “requirement of employment” standard that applies to all non-residents whose base of operation is the employer’s location within Philadelphia. Under this standard, a non- resident employee is not subject to the Wage Tax when the employer requires him or her to perform a job outside of Philadelphia including working from home.A non-resident employee who works from home for his or her convenience is not exempt from the Wage Tax – even with his or her employer’s authorization. On the other hand, if a Philadelphia employer requires a non-resident to perform duties outside the city, he or she is exempt from the Wage Tax for the days spent fulfilling that work.Non-resident employees who had Wage Tax withheld during the time they were required to perform their duties from home in 2020, may file for a refund with a Wage Tax refund petition in 2021. Employees file for a refund after the end of the tax year and will need to provide a copy of their W-2 form. Employees must provide a letter from their employer stating they were required to work from home. The letter, which must be on company letterhead and signed, is submitted along with the refund petition.The City requires an employer to withhold and remit Wage Tax for all its Philadelphia residents, regardless of where they perform their duties./Volumes/prasanna/crawler/pdfs/11.pdf"
)

semantic_text_splitter = AI21SemanticTextSplitter()
document = Document(page_content=TEXT, metadata={"hello": "goodbye"})
documents = semantic_text_splitter.split_documents([document])
print(f"The document list has been split into {len(documents)} Documents.")
for doc in documents:
    print(f"text: {doc.page_content}")
    print(f"metadata: {doc.metadata}")
    print("====")

# COMMAND ----------


from langchain_ai21 import AI21SemanticTextSplitter
from langchain_core.documents import Document
import re

def find_year(doc_url):
  splits = re.split("\/",doc_url)
  fileName = splits[len(splits)-1]
  year = re.findall("[2][0-9][0-9][0-4]",fileName)
  return {'file_name':fileName, 'year':year}

def make_chunks(content, id, parent_url, doc_url):
  file_year = find_year(doc_url)
  document = Document(page_content=content, metadata={"file": file_year['file_name'], "year": file_year['year']})
  documents = semantic_text_splitter.split_documents([document])
  print('id ',id, 'file_year ',file_year )
  chunks = []
  for doc in documents:
    #print(f"text: {doc.page_content}")
    #print(f"metadata: {doc.metadata}")
    chunk = [doc.page_content, doc.metadata]
    chunks.append(chunk)
  return chunks

semantic_text_splitter = AI21SemanticTextSplitter(chunk_size=500)
df1 = spark.read.table('philly_docs_v1')
rows = df1.collect();
data = []
for row in rows:
  #print(row['id'],row['parent_url'],row['doc_url'],row["html_pdf_content"])
  if(row["pdf_content"] is not None):
    if(row["pdf_content"].startswith('--') ): 
      data.append([row["pdf_content"], row['id'],row['parent_url'],row['doc_url'], row["pdf_content"]])
    else:
      chunks = make_chunks(row["pdf_content"], row['id'],row['parent_url'],row['doc_url'] )
      for chunk in chunks:
        data.append([row["pdf_content"], row['id'],row['parent_url'],row['doc_url'], chunk])
df2 = spark.createDataFrame(data, schema="html_pdf_content STRING, id LONG, parent_url STRING, doc_url STRING, chunks STRING")
display(df2)
df2.write.mode("overwrite").saveAsTable("philly_docs_v1_final")





  

# COMMAND ----------

from pyspark.sql.functions import explode, monotonically_increasing_id

df3 = spark.read.table('philly_docs_v1_final')
df4 = df3.select(df3.chunks.alias("chunks"), df3.id.alias('doc_id'), df3.html_pdf_content, df3.doc_url, df3.parent_url, monotonically_increasing_id().alias('id'))
display(df4)
df4.write.mode("overwrite").saveAsTable("philly_docs_v2_final")


# COMMAND ----------


