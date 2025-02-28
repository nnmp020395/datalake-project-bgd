import boto3
import pandas as pd
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Configuration AWS S3
S3_BUCKET_NAME = "data-lake-telecom1"
S3_PARQUET_PATH = "data_L2/20250227/Polluant=NO2/FR_E2_GPS_20250227_L2.parquet/"

# Connexion à Elasticsearch Cloud
ES_CLOUD_ID = "Polluant_dashboard:dXMtZWFzdC0xLmF3cy5mb3VuZC5pbzo0NDMkYmEyZTg4ZWVkNTJlNDQ4MWEwY2E3YzE4ZWFkNDMxNzQkZDhkZjUxMWQ4Y2YzNGEwYWE0Zjk0MGJkZmFiN2E3N2Q="  # 📌 Trouvé dans Elastic Cloud
ES_USERNAME = "elastic"
ES_PASSWORD = "KBtXGBOpRFGI3oxWaeVlewbp"
ES_INDEX = "pollution_data"

# Initialiser la connexion Elasticsearch Cloud
es = Elasticsearch(
    cloud_id=ES_CLOUD_ID,
    basic_auth=(ES_USERNAME, ES_PASSWORD)
)

# Vérifier la connexion
print(es.info())

conn = BaseHook.get_connection("aws_default")
aws_access_id = conn.login
aws_access_secret = conn.password
print('--- Connexion à AWS S3 ---')
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_id,
    aws_secret_access_key=aws_access_secret
)

# Initialiser Spark
spark = SparkSession.builder \
    .appName("ReadParquetFromS3") \
    .config("spark.hadoop.fs.s3a.access.key", "VOTRE_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "VOTRE_SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# Lire le fichier Parquet depuis S3
df_spark = spark.read.parquet(f"s3a://{S3_BUCKET_NAME}/{S3_PARQUET_PATH}")

# Convertir en Pandas
df_pandas = df_spark.toPandas()

# Indexer chaque document dans Elasticsearch Cloud
for _, row in df_pandas.iterrows():
    document = row.to_dict()
    res = es.index(index=ES_INDEX, document=document)
    print(f"--- Document ajouté : {res['_id']} ---")

print(f"--- Données stockées dans Elasticsearch Cloud sous l'index `{ES_INDEX}` ---")
