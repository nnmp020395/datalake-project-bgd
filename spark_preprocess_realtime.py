from pyspark.sql import SparkSession
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import lit
import boto3
import sys
import os

import datetime as dt
current_date = dt.datetime.now()
# current_date = current_date - dt.timedelta(days=1)

# Récupérer les informations de connexion à S3 de la connexion Airflow
conn = BaseHook.get_connection("aws_default")
aws_access_id = conn.login
aws_access_secret = conn.password
print('--- Connexion à AWS S3 ---')

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

# Initialisation
S3_BUCKET_NAME = "data-lake-telecom1"
S3_INPUT_PATH =  f"s3a://{S3_BUCKET_NAME}/data_L0/FR_E2_{current_date.strftime("%Y-%m-%d")}.csv"
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_id,
    aws_secret_access_key=aws_access_secret
)


# Fonction pour créer un "dossier" S3
def create_s3_folder(bucket_name, folder_name):
    s3.put_object(Bucket=bucket_name, Key=(folder_name + '/'))

# Fonction pour vérifier si un dossier existe dans S3
def check_s3_folder_exists(bucket_name, folder_name):
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=folder_name,
        Delimiter='/')
    return 'Contents' in response

# Lire les données depuis S3
df = spark.read.option("delimiter", ";").csv(S3_INPUT_PATH, header=True, inferSchema=True)
print(f"--- Données chargées depuis S3 : {S3_INPUT_PATH} ---")
polluants = df.select("Polluant").distinct().rdd.flatMap(lambda x: x).collect()
print(f"--- Liste des polluants : {polluants} ---")

# Créer un dossier pour stocker les données nettoyées
S3_BUCKET_TO_CHECK = f"{S3_BUCKET_NAME}/data_L1/{current_date.strftime("%Y%m%d")}"
if not check_s3_folder_exists(S3_BUCKET_NAME, f"data_L1/{current_date.strftime("%Y%m%d")}"):
    create_s3_folder(S3_BUCKET_NAME, f"data_L1/{current_date.strftime("%Y%m%d")}")
    print(f"--- Dossier data_L1/{current_date.strftime("%Y%m%d")} créé ---")



for polluant in polluants:
    NEW_FOLDER_POLLUANT = f"data_L1/{current_date.strftime("%Y%m%d")}/{polluant}/"
    if not check_s3_folder_exists(S3_BUCKET_NAME, NEW_FOLDER_POLLUANT):
        create_s3_folder(S3_BUCKET_NAME, NEW_FOLDER_POLLUANT)
        print(f"--- Dossier polluant créé : {NEW_FOLDER_POLLUANT} ---")

# S3_OUTPUT_PATH = f"s3a://{S3_BUCKET_NAME}/data_L1/{NEW_FOLDER_POLLUANT}/FR_E2_{current_date.strftime("%Y-%m-%d")}.csv"
# Sélectionner les colonnes utiles
columns_selected = ['Date de début', 'Date de fin', 'code zas', 'Zas',
                    'code site', 'nom site', "type d'implantation", 'Polluant',
                    "type d'influence", 'valeur', 'valeur brute', 'unité de mesure', 'validité']
df = df.select(columns_selected)
# Nettoyage des données (suppression des valeurs nulles)
df_cleaned = df.dropna(subset=['Date de début', 'Date de fin', 'valeur', 'valeur brute'])
# GroupBy Zas et Polluant et sauvegarder les données en fichiers Parquet distincts
grouped_df = df_cleaned.groupBy("Zas", "Polluant").count().show()

df_cleaned.repartition("Zas", "Polluant") \
    .write.mode("overwrite") \
    .partitionBy("Polluant") \
    .option("compression", "snappy") \
    .parquet(f"s3a://{S3_BUCKET_NAME}/data_L1/{current_date.strftime("%Y%m%d")}/")
print("--- Données nettoyées et enregistrées sur S3 ---")

#-----------------------------------------------------------
#-----------------------------------------------------------

# Arrêter la session Spark
spark.stop()
