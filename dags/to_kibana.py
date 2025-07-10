import boto3
import pandas as pd
from elasticsearch import Elasticsearch, helpers
from pyspark.sql import SparkSession
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType
from pyspark.sql.types import TimestampType, FloatType, StringType, MapType, BinaryType
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import array, decode, col, udf, date_format

import datetime as dt
current_date = dt.datetime.now()
# current_date = current_date - dt.timedelta(days=4)

# Connexion à AWS S3
conn = BaseHook.get_connection("aws_default")
aws_access_id = conn.login
aws_access_secret = conn.password
print('--- Connexion à AWS S3 ---')
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_id,
    aws_secret_access_key=aws_access_secret
)
print('--- Configurer client AWS S3 ---')

# Définition de la configuration Spark
conf = SparkConf() \
    .setAppName("PreprocessRealtimeData") \
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .set("spark.hadoop.fs.s3a.access.key", aws_access_id) \
    .set("spark.hadoop.fs.s3a.secret.key", aws_access_secret) \
    .set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")

# Initialisation de SparkContext
sc = SparkContext(conf=conf)
print("SparkContext Initialisé :", sc.appName)

# Initialisation de la session Spark avec la configuration S3
spark = SparkSession.builder \
    .appName("PreprocessRealtimeData") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_access_secret) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()
    # .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    # .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
print("--- Session Spark initialisée ---")


# Connexion à Elasticsearch Cloud
elastic_conn_od = "elasticsearch_default"
conn_elastic = BaseHook.get_connection("elasticsearch_default")
elastic_host = conn_elastic.host
elastic_port = conn_elastic.port
elastic_user = conn_elastic.login
elastic_password = conn_elastic.password

# es = Elasticsearch(
#     hosts=[f"http://{elastic_host}:{elastic_port}"],
#     basic_auth=(elastic_user, elastic_password)
# )
client = Elasticsearch(
  "https://24b83bdf40114e39b1399f4b819ef89b.us-east-1.aws.found.io:443",
  api_key="bGZKYVVwVUJmZ1BDQWZleTVmbmM6em1CQVZtT1ZUNmFBMDNtb2NDaHFwdw=="
)
# # API key should have cluster monitor rights
# client.info()
print('--- Connexion à Elasticsearch réussie ---')


#------------------------------------------------------------------------
# Fonction pour lister tous les fichiers Parquet dans un bucket S3
def list_all_parquet_files(bucket, prefix):
    """Liste tous les fichiers Parquet dans un bucket S3 avec pagination"""
    files = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if "Contents" in page:
            for obj in page["Contents"]:
                if obj["Key"].endswith(".parquet"):
                    files.append(f"s3a://{bucket}/{obj['Key']}")
    return files

#------------------------------------------------------------------------
# Configuration le schéma du Dataframe
# schema = StructType([
#     StructField("Date de début", TimestampType(), True),
#     StructField("Date de fin", TimestampType(), True),
#     StructField("code zas", StringType(), True),
#     StructField("Zas", StringType(), True),
#     StructField("code site", StringType(), True),
#     StructField("nom site", StringType(), True),
#     StructField("type d'implantation", StringType(), True),
#     StructField("Polluant", StringType(), True),
#     StructField("type d'influence", StringType(), True),
#     StructField("valeur", DoubleType(), True),
#     StructField("valeur brute", DoubleType(), True),
#     StructField("unité de mesure", StringType(), True),
#     StructField("validité", IntegerType(), True),
#     StructField("nuts3", StringType(), True),
#     StructField("nuts2", StringType(), True),
#     StructField("NatlStationCode", StringType(), True),
#     StructField("Name", StringType(), True),
#     StructField("Latitude", DoubleType(), True),
#     StructField("Longitude", DoubleType(), True),
#     # StructField("GPS",  StringType(), True)
# ])
# print('--- Configuration du schéma finie ---')
# Date de début	Date de fin	code zas	Zas	code site	nom site	type d'implantation	Polluant	type d'influence	valeur	valeur brute	unité de mesure	validité
# nuts3	nuts2	NatlStationCode	Name	Latitude	Longitude	GPS
#------------------------------------------------------------------------
# Read the data from S3


S3_BUCKET_NAME = "data-lake-telecom1"
S3_PREFIX = f"data_L2/{current_date.strftime('%Y%m%d')}"

parquet_files = list_all_parquet_files(S3_BUCKET_NAME, S3_PREFIX)
if not parquet_files:
    print("--- Aucun fichier Parquet trouvé ---")
    exit()

print(f"{len(parquet_files)} fichiers Parquet trouvés")


# # Lecture optimisée des fichiers Parquet
# df = spark.read.schema(schema).parquet(*parquet_files).repartition(8)  # Améliore la performance en batch
# print(df.printSchema())
# print("--- Chargé tous les fichiers Parquet ---")
# # Convertir la colonne GPS en double
# def convert_gps_to_map(gps_str):
#     import json
#     gps_map = json.loads(gps_str)
#     return {float(k): float(v) for k, v in gps_map.items()}

# # convert_gps_udf = udf(convert_gps_to_map, MapType(FloatType(), FloatType()))
# # df = df.withColumn("GPS", convert_gps_udf(col("GPS")))
# # df = df.withColumn("Date de début", decode("Date de début", 'UTF-8').cast('timestamp'))
# # df = df.withColumn("Date de fin", decode("Date de fin", 'UTF-8').cast('timestamp'))
# df = df.withColumn("date", df["Date de début"].cast("timestamp"))
# df = df.withColumn("mapping_id", array(df["date"], df["code site"], df["Polluant"]))
# print(df.show(5))
# # print("--- Count NaT ---", df.dropna(subset=["Date de début", "Date de fin"]).count())
# # df_pandas = df.dropna().toPandas()
# # print("--- Conversion en Pandas ---")
# # print(df_pandas.head(5))
# print("--- Nombre de lignes avant `dropna()` ---", df.count())
# df = df.dropna()
# print("--- Nombre de lignes après `dropna()` ---", df.count())
# print("--- Schéma Spark ---")
# df.printSchema()
# print("--- Vérification des valeurs extrêmes ---")
# df.describe().show()
# df_pandas = df.toPandas()
# print("--- Conversion en Pandas ---")


df = spark.read.parquet(*parquet_files).repartition(8)

# ✅ Vérifier si les données sont bien chargées
print("✅ Nombre de lignes avant traitement :", df.count())

# ✅ Convertir les colonnes importantes en format lisible par Elasticsearch
df = df.withColumn("Date de début", date_format(col("Date de début"), "yyyy-MM-dd'T'HH:mm:ss"))
df = df.withColumn("Date de fin", date_format(col("Date de fin"), "yyyy-MM-dd'T'HH:mm:ss"))
df = df.withColumn("Latitude", col("Latitude").cast("double"))
df = df.withColumn("Longitude", col("Longitude").cast("double"))
df = df.withColumn("GPS", array(col("Longitude"), col("Latitude")))

# ✅ Suppression des valeurs `NULL`
df = df.dropna()

print("✅ Nombre de lignes après nettoyage :", df.count())
print(df.show(5))


# Fonction pour l'indexation en batch dans Elasticsearch
def bulk_index(df, index_name):
    """Envoi des données en batch dans Elasticsearch"""
    # Vérification si le DataFrame est vide
    if df.count() == 0:
        print("--- Aucune donnée à indexer ---")
        return
    actions = [
        {
            "_op_type": "index", # Opération d'indexation
            "_index": index_name,
            "_id": f"{row['date']}_{row['code site']}_{row['Polluant_copy']}",
            "_source": row.asDict()
        }
        for row in df.collect()
    ]
    try:
        helpers.bulk(client, actions, raise_on_error=False)
        #client.index(index=index_name, id=actions[0]["_id"], document=actions[0]["_source"])
    except Exception as e:
        print(f"--- Erreur lors de l'indexation : {e} ---")
        return
    # print(f"{len(df)} documents indexés dans `{index_name}`")

# Indexer les données en batch dans Elasticsearch
bulk_index(df, "polluants")

print("--- Données indexées dans Elasticsearch Cloud ---")

# Vérifier les données indexées
# response = es.search(index="pollution_data", size=5)  # Affiche les 5 premiers document
# print("--- Vérification ---",response["hits"]["hits"])  # Affichage des résultats


# for file in parquet_files:
#     df = spark.read.schema(schema).parquet(file)
#     print(f"--- Lecture Parquet file {file} ---")
#     df = df.withColumn("date", df["Date de début"].cast("date"))
#     df = df.withColumn("mapping_id", array(df["date"], df["code site"], df["Polluant"]))
#     # indexer chaque document dans Elasticsearch Cloud
#     df.write \
#         .format("org.elasticsearch.spark.sql") \
#         .option("es.nodes", elastic_host) \
#         .option("es.port", elastic_port) \
#         .option("es.net.http.auth.user", elastic_user) \
#         .option("es.net.http.auth.pass", elastic_password) \
#         .option("es.nodes.wan.only", "true") \
#         .option("es.resource", "pollution_data") \
#         .option("es.mapping.id", "mapping_id") \
#         .option("es.index.auto.create", "false") \
#         .mode("overwrite") \
#         .save()
# print("--- Données indexées dans Elasticsearch Cloud ---")


spark.stop()