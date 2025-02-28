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
PARQUET_INPUT =  f"s3a://{S3_BUCKET_NAME}/data_L1/{current_date.strftime('%Y%m%d')}"
CSV_INPUT_PATH =  f"s3a://{S3_BUCKET_NAME}/data_L1/maps/zas_region_gps.csv"
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_id,
    aws_secret_access_key=aws_access_secret
)


import pyspark.pandas as ps

# Fonction pour créer un "dossier" S3
def create_s3_folder(bucket_name, folder_name):
    s3_client.put_object(Bucket=bucket_name, Key=(folder_name + '/'))

# Fonction pour vérifier si un dossier existe dans S3
def check_s3_folder_exists(bucket_name, folder_name):
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=folder_name,
        Delimiter='/')
    return 'Contents' in response

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

# Fonction pour combiner les données (Parquet) et GPS (CSV)
def combine_datasets(parquetfile, csvfile, parquet_column, csv_column):
    """Effectue une jointure entre les données Parquet et GPS"""
    df_parquet = ps.read_parquet(parquetfile)
    df_csv = ps.read_csv(csvfile)
    df_joined = df_parquet.set_index(parquet_column).join(df_csv.set_index(csv_column))
    print("--- Jointure effectuée ---")

    # save joined data to S3
    polluant = parquetfile.split("/")[5]
    S3_OUTPUT_PATH = f"s3a://{S3_BUCKET_NAME}/data_L2/{current_date.strftime('%Y%m%d')}/{polluant}"
    # check folde exists
    if not check_s3_folder_exists(S3_BUCKET_NAME, f"data_L2/{current_date.strftime('%Y%m%d')}/{polluant}"):
        create_s3_folder(S3_BUCKET_NAME, f"data_L2/{current_date.strftime('%Y%m%d')}/{polluant}")
        print(f"--- Dossier data_L2/{current_date.strftime('%Y%m%d')}/{polluant} créé ---")

    df_joined['date'] = current_date.strftime('%Y-%m-%d')
    df_joined.to_parquet(
        f'{S3_OUTPUT_PATH}/FR_E2_GPS_{current_date.strftime('%Y%m%d')}_L2.parquet',
        mode='overwrite'
    )
    print(f"--- Résultat enregistré sur S3 : {S3_OUTPUT_PATH} ---")

    return df_joined



S3_PREFIX = "data_L1/"
# 📌 Exécution de la fonction
parquet_files = list_all_parquet_files(S3_BUCKET_NAME, S3_PREFIX)

# 📌 Affichage des fichiers trouvés
print("✅ Fichiers Parquet trouvés :")
for file in parquet_files:
    print(f"--- Parquet file: {file} ---")
    df_combined = combine_datasets(file, CSV_INPUT_PATH, "code site", "NatlStationCode")

# Arrêter la session Spark
spark.stop()



# polluants = ["C6H6", "NO", "NO2", "NOX as NO2","PM10", "PM2.5", "O3", "SO2", "CO"]
# for polluant in polluants:
#     parquet_path = f"{PARQUET_INPUT}/Polluant={polluant}/.parquet"
#     df_parquet = ps.read_parquet(parquet_path)
#     print(f"--- Parquet chargé depuis S3 ---")
# parquet_path="s3://data-lake-telecom1/data_L1/20250225/Polluant=C6H6/part-00000-63a55509-188a-4721-978b-1cb7b733f7e3.c000.snappy.parquet"

# # Lecture des données csv
# df_gps = ps.read_csv(CSV_INPUT_PATH)
# print("--- GPS.csv chargé depuis S3 ---")

# # Jointure des données par code région
# df_joined = df_parquet.set_index('code site').join(df_gps.set_index('NatlStationCode'))
# print("--- Jointure effectuée ---")

# df_joined.show(5)
# S3_OUTPUT_PATH = f"s3a://{S3_BUCKET_NAME}/data_L2/{current_date.strftime('%Y%m%d')}"
# # 📌 Sauvegarde du DataFrame joint sur S3 au format Parquet avec compression Snappy
# df_joined.write.mode("overwrite") \
#     .option("compression", "snappy") \
#     .parquet(S3_OUTPUT_PATH)

# df_joined.to_parquet(
#     '%s/FR_E2_GPS_20250225_C6H6.parquet' % S3_OUTPUT_PATH,
#     mode='overwrite',
#     partition_cols='date'
# )
# print(f"--- Résultat enregistré sur S3 : {S3_OUTPUT_PATH} ---")
