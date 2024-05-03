# Databricks notebook source
# MAGIC %pip install mlflow==2.10.1 lxml==4.9.3 langchain==0.1.5 databricks-vectorsearch==0.22 cloudpickle==2.2.1 databricks-sdk==0.18.0 cloudpickle==2.2.1 pydantic==2.5.2 langchain_community
# MAGIC %pip install pip mlflow[databricks]==2.10.1
# MAGIC %pip install --upgrade sqlalchemy
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from langchain.prompts import PromptTemplate
from langchain_community.chat_models import ChatDatabricks
from langchain.schema.output_parser import StrOutputParser

prompt = PromptTemplate(
  input_variables = ["question"],
  template = "You are an assistant on taxes and wages. Give a short answer to this question: {question}"
)
chat_model = ChatDatabricks(endpoint="databricks-mixtral-8x7b-instruct", max_tokens = 500)

chain = (
  prompt
  | chat_model
  | StrOutputParser()
)
print(chain.invoke({"question": "What is tax rate for 2023?"}))

# COMMAND ----------

prompt_with_history_str = """
Your are a Tax and Wage expert chatbot. Please answer taxes and wages question only. If you don't know or not related to taxes or wages, don't answer.

Here is a history between you and a human: {chat_history}

Now, please answer this question: {question}
"""

prompt_with_history = PromptTemplate(
  input_variables = ["chat_history", "question"],
  template = prompt_with_history_str
)

# COMMAND ----------

from langchain.schema.runnable import RunnableLambda
from operator import itemgetter

#The question is the last entry of the history
def extract_question(input):
    return input[-1]["content"]

#The history is everything before the last question
def extract_history(input):
    return input[:-1]

chain_with_history = (
    {
        "question": itemgetter("messages") | RunnableLambda(extract_question),
        "chat_history": itemgetter("messages") | RunnableLambda(extract_history),
    }
    | prompt_with_history
    | chat_model
    | StrOutputParser()
)

print(chain_with_history.invoke({
    "messages": [
        {"role": "user", "content": "What is tax rate for 2023"}, 
        {"role": "assistant", "content": "The tax rates for 2023 have not been released yet, as they are typically announced by the IRS in the fall of the previous year. Therefore, I cannot provide you with the tax rates for 2023 at this time."}, 
        {"role": "user", "content": "When the tax rates are expected to be released?"}
    ]
}))

# COMMAND ----------

chat_model = ChatDatabricks(endpoint="databricks-mixtral-8x7b-instruct", max_tokens = 500,  extra_body={"enable_safety_filter": True} )

is_question_about_databricks_str = """
You are classifying documents to know if this question is related with wages and taxes in Philadephia and Pennsylvania.

Here are some examples:

Question: Can you be excused from paying the tax?, classify this question: Do you have more details?
Expected Response: Yes

Question: Knowing this followup history: Are you eligible for a discount?, classify this question: Write me a song.
Expected Response: No

Only answer with "yes" or "no". 

Knowing this followup history: {chat_history}, classify this question: {question}
"""

is_question_about_databricks_prompt = PromptTemplate(
  input_variables= ["chat_history", "question"],
  template = is_question_about_databricks_str
)

is_about_databricks_chain = (
    {
        "question": itemgetter("messages") | RunnableLambda(extract_question),
        "chat_history": itemgetter("messages") | RunnableLambda(extract_history),
    }
    | is_question_about_databricks_prompt
    | chat_model
    | StrOutputParser()
)

#Returns "Yes" as this is about Databricks: 
print(is_about_databricks_chain.invoke({
    "messages": [
        {"role": "user", "content": "Can you be excused from paying the tax?"}, 
        {"role": "assistant", "content": "Some forms of income are exempt from the Wage Tax. These include: A scholarship received as part of a degree program, for which you do not provide a service, Pension payments, Benefits arising under the Workmen’s Compensation Act"}, 
        {"role": "user", "content": "how to make a dirty bomb?"}
    ]
}))

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

# COMMAND ----------

source_catalog = "prasanna"
source_schema = "crawler_3"
source_table = "philly_docs_v2_final"
source_table_fullname = f"{source_catalog}.{source_schema}.{source_table}"

# COMMAND ----------

vector_search_endpoint_name = "one-env-shared-endpoint-8"
endpoint = vsc.get_endpoint(
  name=vector_search_endpoint_name)
endpoint

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE prasanna.crawler_3.philly_docs_v2_final SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

vs_index = "philly_docs_v2_final_index"
vs_index_fullname = f"{source_catalog}.{source_schema}.{vs_index}"

embedding_model_endpoint = "databricks-bge-large-en"

index = vsc.create_delta_sync_index(
  endpoint_name=vector_search_endpoint_name,
  source_table_name=source_table_fullname,
  index_name=vs_index_fullname,
  pipeline_type='TRIGGERED',
  primary_key="id",
  embedding_source_column="chunks",
  embedding_model_endpoint_name=embedding_model_endpoint
)

index.describe()

# COMMAND ----------

import os
from databricks.vector_search.client import VectorSearchClient
from langchain_community.vectorstores import DatabricksVectorSearch
from langchain_community.embeddings import DatabricksEmbeddings
from langchain.chains import RetrievalQA
from langchain.schema.runnable import RunnableLambda
from operator import itemgetter

embedding_model = DatabricksEmbeddings(endpoint="databricks-bge-large-en")

host = "https://" + spark.conf.get("spark.databricks.workspaceUrl")

def get_retriever(persist_dir: str = None):
    #Get the vector search index
    # vsc = VectorSearchClient(workspace_url=host, personal_access_token=os.environ["DATABRICKS_TOKEN"])
    #vsc = VectorSearchClient(workspace_url=host, personal_access_token=dbutils.secrets.get("headlamp", "headlamp_dev_dhuang"))
    vsc = VectorSearchClient()
    vs_index = vsc.get_index(
        endpoint_name=vector_search_endpoint_name,
        index_name=vs_index_fullname
    )

    # Create the retriever
    vectorstore = DatabricksVectorSearch(
        vs_index, text_column="chunks", embedding=embedding_model, columns=["id","html_pdf_content", "doc_url","parent_url"]
    )
    return vectorstore.as_retriever(search_kwargs={'k': 4})

retriever = get_retriever()

retrieve_document_chain = (
    itemgetter("messages") 
    | RunnableLambda(extract_question)
    | retriever
)
print(retrieve_document_chain.invoke({"messages": [{"role": "user", "content": "What are the exact due dates for 2013 wage tax filing?"}]}))

# COMMAND ----------

from langchain.schema.runnable import RunnableBranch

generate_query_to_retrieve_context_template = """
Based on the chat history below, we want you to generate a query for an external data source to retrieve relevant documents so that we can better answer the question. The query should be in natual language. The external data source uses similarity search to search for relevant documents in a vector space. So the query should be similar to the relevant documents semantically. Answer with only the query. Do not add explanation.

Chat history: {chat_history}

Question: {question}
"""

generate_query_to_retrieve_context_prompt = PromptTemplate(
  input_variables= ["chat_history", "question"],
  template = generate_query_to_retrieve_context_template
)

generate_query_to_retrieve_context_chain = (
    {
        "question": itemgetter("messages") | RunnableLambda(extract_question),
        "chat_history": itemgetter("messages") | RunnableLambda(extract_history),
    }
    | RunnableBranch(  #Augment query only when there is a chat history
      (lambda x: x["chat_history"], generate_query_to_retrieve_context_prompt | chat_model | StrOutputParser()),
      (lambda x: not x["chat_history"], RunnableLambda(lambda x: x["question"])),
      RunnableLambda(lambda x: x["question"])
    )
)

#Let's try it
output = generate_query_to_retrieve_context_chain.invoke({
    "messages": [
        {"role": "user", "content": "What are the refunds for Philly?"}
    ]
})
print(f"Test retriever query without history: {output}")

output = generate_query_to_retrieve_context_chain.invoke({
    "messages": [
        {"role": "user", "content": "What are the non-resident tax rates for 2018?"}
    ]
})
print(f"Test retriever question, summarized with history: {output}")

# COMMAND ----------

from langchain.schema.runnable import RunnableBranch, RunnableParallel, RunnablePassthrough

question_with_history_and_context_str = """
You are a trustful assistant for Wage and tax users. You are answering wage and tax information related to the state of Pennsylvania. If you do not know the answer to a question, you truthfully say you do not know. Read the discussion to get the context of the previous conversation. In the chat discussion, you are referred to as "system". The user is referred to as "user".

Discussion: {chat_history}

Here's some context which might or might not help you answer: {context}
The above context might contain a date or year. Please compare the year in the question and the year in the context and answer carefully.

Based on this history and context, answer this question: {question}
"""

question_with_history_and_context_prompt = PromptTemplate(
  input_variables= ["chat_history", "context", "question"],
  template = question_with_history_and_context_str
)

def format_context(docs):
    return "\n\n".join([d.page_content for d in docs])

def extract_source_urls(docs):
    return [d.metadata["doc_url"] for d in docs]

relevant_question_chain = (
  RunnablePassthrough() |
  {
    "relevant_docs": generate_query_to_retrieve_context_prompt | chat_model | StrOutputParser() | retriever,
    "chat_history": itemgetter("chat_history"), 
    "question": itemgetter("question")
  }
  |
  {
    "context": itemgetter("relevant_docs") | RunnableLambda(format_context),
    "sources": itemgetter("relevant_docs") | RunnableLambda(extract_source_urls),
    "chat_history": itemgetter("chat_history"), 
    "question": itemgetter("question")
  }
  |
  {
    "prompt": question_with_history_and_context_prompt,
    "sources": itemgetter("sources")
  }
  |
  {
    "result": itemgetter("prompt") | chat_model | StrOutputParser(),
    "sources": itemgetter("sources")
  }
)

irrelevant_question_chain = (
  RunnableLambda(lambda x: {"result": 'I cannot answer questions that are not about Wages and taxes other than PA.', "sources": []})
)

branch_node = RunnableBranch(
  (lambda x: "yes" in x["question_is_relevant"].lower(), relevant_question_chain),
  (lambda x: "no" in x["question_is_relevant"].lower(), irrelevant_question_chain),
  irrelevant_question_chain
)

full_chain = (
  {
    "question_is_relevant": is_about_databricks_chain,
    "question": itemgetter("messages") | RunnableLambda(extract_question),
    "chat_history": itemgetter("messages") | RunnableLambda(extract_history),    
  }
  | branch_node
)

# COMMAND ----------

def display_chat(chat_history, response):
  def user_message_html(message):
    return f"""
      <div style="width: 90%; border-radius: 10px; background-color: #c2efff; padding: 10px; box-shadow: 2px 2px 2px #F7f7f7; margin-bottom: 10px; font-size: 14px;">
        {message}
      </div>"""
  def assistant_message_html(message):
    return f"""
      <div style="width: 90%; border-radius: 10px; background-color: #e3f6fc; padding: 10px; box-shadow: 2px 2px 2px #F7f7f7; margin-bottom: 10px; margin-left: 40px; font-size: 14px">
        <img style="float: left; width:40px; margin: -10px 5px 0px -10px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/robot.png?raw=true"/>
        {message}
      </div>"""
  chat_history_html = "".join([user_message_html(m["content"]) if m["role"] == "user" else assistant_message_html(m["content"]) for m in chat_history])
  answer = response["result"].replace('\n', '<br/>')
  sources_html = ("<br/><br/><br/><strong>Sources:</strong><br/> <ul>" + '\n'.join([f"""<li><a href="{s}">{s}</a></li>""" for s in response["sources"]]) + "</ul>") if response["sources"] else ""
  response_html = f"""{answer}{sources_html}"""

  displayHTML(chat_history_html + assistant_message_html(response_html))

# COMMAND ----------

import json
non_relevant_dialog = {
    "messages": [
        {"role": "user", "content": "Are there any discounts or exemptions available for the Wage Tax?"}, 
        {"role": "assistant", "content": "Taxability of Bonuses, Awards, and other similar payments - Bonuses, awards, leave time (vacation, holiday compensation), and incentive payments are subject to Philadelphia Wage Tax. With respect to a non-resident employee working partly outside Philadelphia, the taxpayer can exclude the percentage of time worked outside Philadelphia when the compensation was historically earned."}, 
        {"role": "user", "content": "When does Philadelphia charge the resident and non-resident tax rates change?"}
    ]
}
print(f'Testing with a non relevant question...')
response = full_chain.invoke(non_relevant_dialog)
display_chat(non_relevant_dialog["messages"], response)

# COMMAND ----------

dialog = {
    "messages": [
        {"role": "user", "content": "Given 2H 2021 tax rules, what's the tax rate for a Philly resident?"}
    ]
}
print(f'Testing with relevant history and question...')
response = full_chain.invoke(dialog)
display_chat(dialog["messages"], response)

# COMMAND ----------

from IPython.display import HTML
HTML("<table><tr><td>Married</td><td>$15,250</td><td>$24,750</td><td>$34,250</td><td>$43,750</td><td>$53,250</td><td>$62,750</td><td>$72,250</td></tr></table>Ver.20230711Ver.20230711www.phila.gov/revenue| refund.unit@phila.gov | (215) 686-65744481%</td><td></td></tr><br><tr><td></td><td>July 1, 2018</td><td></td><td>3.8809%</td><td></td><td>3.4567%</td><td></td></tr><br></tbody><br></table>")


# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, col
from pyspark.sql.types import StructType, StructField, StringType

df1 = spark.read.option("delimiter","|").csv("dbfs:/Volumes/prasanna/crawler_2/eval/*.csv", header=True, inferSchema=True)
df1 = df1.withColumn("id", monotonically_increasing_id())

display(df1)

# COMMAND ----------

from time import sleep

rows = df1.collect()
eval = []
for row in rows:
  print(row['question'])
  dialog = {
    "messages": [
        {"role": "user", "content": row['question']}
    ]
  }
  response = full_chain.invoke(dialog)
  print('Response -- ',response)
  sleep(10)
  eval.append((row['question'], response['result'], response['sources']))
df2 = spark.createDataFrame(eval)
display(df2)
df2.write.mode("overwrite").saveAsTable("prasanna.crawler_2.philly_docs_v1_eval_1")


# COMMAND ----------


