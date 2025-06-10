import boto3
import pandas as pd
from elasticsearch import Elasticsearch, helpers

from airflow.hooks.base import BaseHook

# Connexion à Elasticsearch Cloud
# elastic_conn_od = "elasticsearch_default"
conn_elastic = BaseHook.get_connection("elasticsearch_default")
elastic_host = conn_elastic.host
elastic_port = conn_elastic.port
elastic_user = conn_elastic.login
elastic_password = conn_elastic.password

es = Elasticsearch(
    hosts=[f"http://{elastic_host}:{elastic_port}"],
    basic_auth=(elastic_user, elastic_password)
)
print('--- Connexion à Elasticsearch réussie ---')

response = es.search(index="pollution_data", size=5)  # 🔍 Affiche les 5 premiers documents
print(response["hits"]["hits"])  # 📌 Affichage des résultats

from elasticsearch import Elasticsearch

client = Elasticsearch(
  "https://24b83bdf40114e39b1399f4b819ef89b.us-east-1.aws.found.io:443",
  api_key="UGZLZlVaVUJmZ1BDQWZleTNmbHA6RWtOQjg1b2VTWmlSSXVSVFpKWGl6QQ=="
)

# API key should have cluster monitor rights
client.info()

documents = [
  { "index": { "_index": "polluants", "_id": "9780553351927"}},
  {"name": "Snow Crash", "author": "Neal Stephenson", "release_date": "1992-06-01", "page_count": 470, "_extract_binary_content": true, "_reduce_whitespace": true, "_run_ml_inference": true},
  { "index": { "_index": "polluants", "_id": "9780441017225"}},
  {"name": "Revelation Space", "author": "Alastair Reynolds", "release_date": "2000-03-15", "page_count": 585, "_extract_binary_content": true, "_reduce_whitespace": true, "_run_ml_inference": true},
  { "index": { "_index": "polluants", "_id": "9780451524935"}},
  {"name": "1984", "author": "George Orwell", "release_date": "1985-06-01", "page_count": 328, "_extract_binary_content": true, "_reduce_whitespace": true, "_run_ml_inference": true},
  { "index": { "_index": "polluants", "_id": "9781451673319"}},
  {"name": "Fahrenheit 451", "author": "Ray Bradbury", "release_date": "1953-10-15", "page_count": 227, "_extract_binary_content": true, "_reduce_whitespace": true, "_run_ml_inference": true},
  { "index": { "_index": "polluants", "_id": "9780060850524"}},
  {"name": "Brave New World", "author": "Aldous Huxley", "release_date": "1932-06-01", "page_count": 268, "_extract_binary_content": true, "_reduce_whitespace": true, "_run_ml_inference": true},
  { "index": { "_index": "polluants", "_id": "9780385490818"}},
  {"name": "The Handmaid's Tale", "author": "Margaret Atwood", "release_date": "1985-06-01", "page_count": 311, "_extract_binary_content": true, "_reduce_whitespace": true, "_run_ml_inference": true},
]

client.bulk(operations=documents, pipeline="ent-search-generic-ingestion")
client.search(index="polluants", q="snow")