from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from elasticsearch import Elasticsearch
import json
import requests
import boto3
import io
import pandas as pd

import datetime as dt
current_date = dt.datetime.now()
# current_date = current_date - dt.timedelta(days=4)
year = dt.datetime.now().year

# Définir les variables
LOCAL_FILE_PATH = "/Users/phuongnguyen/Documents/cours_BGD_Telecom_Paris_2024/data_lake_project/data_api.csv"
S3_BUCKET_NAME = "data-lake-telecom1"
# S3_FILE_PATH = "data_api/data.csv"
urls=[
        'https://www.arcgis.com/sharing/rest/content/items/28db8af8c5a14b04be8b3f980bc0ee67/data',
        'http://osm13.openstreetmap.fr/~cquest/openfla/export/departements-20180101-shp.zip',
        'https://files.data.gouv.fr/lcsqa/concentrations-de-polluants-atmospheriques-reglementes/temps-reel/',
        'https://osm13.openstreetmap.fr/~cquest/openfla/export/regions-20180101-shp.zip'
        'https://static.data.gouv.fr/resources/donnees-temps-reel-de-mesure-des-concentrations-de-polluants-atmospheriques-reglementes-1/20250207-124113/fr-2025-d-lcsqa-ineris-20250113.xls'
    ]
conn = BaseHook.get_connection('aws_default')
aws_access_id = conn.login
aws_access_secret = conn.password

print('--- Connexion à AWS S3 ---')
#================================================================================================
def fetch_store_data_to_s3(url, headers, S3_FILE_PATH, aws_conn_id='aws_default', content_type='csv', **kwargs):
    """
    Get the data from the API and store it directly in S3

    Args:
        aws_conn_id (str): The AWS connection ID
        **kwargs: Arbitrary keyword arguments
    """
    try:
        print('--- Démarrer la récupération des données depuis API ---')
        r = requests.get(url, timeout=20, stream=True)
        r.raise_for_status()  # vérifier si la requête a réussi code 200
        print("--- Récupération des données réussie ---")

        # Récupérer les identifiants AWS pour la connexion avec Airflow
        conn = BaseHook.get_connection(aws_conn_id)
        aws_access_id = conn.login
        aws_access_secret = conn.password

        print('--- Connexion à AWS S3 ---')
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_id,
            aws_secret_access_key=aws_access_secret,
            region_name=conn.extra_dejson.get('region_name', 'us-east-1')
        )
        print('--- Connexion réussie ---')

        # Déterminer le type de contenu
        if content_type == 'csv':
            object_type = "application/csv"
        elif content_type == 'json':
            object_type = "application/json"
        elif content_type == 'zip':
            object_type = "application/zip"
        else:
            object_type = "text/plain"

        print('--- Envoi des données sur S3 ---')
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=S3_FILE_PATH,
            Body=r.content,
            ContentType=object_type
        )
        print('--- Envoi des données sur S3 réussi ---')
        return S3_FILE_PATH

    except requests.exceptions.RequestException as e:
        print(f'--- Erreur lors de la récupération des données : {e} ---')
    except Exception as e:
        print(f" --- Erreur S3 : {e} ---")

#================================================================================================
S3_BUCKET = "data-lake-telecom1"
# S3_INPUT_KEY = "data_api/data.csv"  # Fichier source
# S3_OUTPUT_KEY = "data_L1/formatted_data_2024_annual.csv"  # Résultat après calculs
# Fonction pour lire S3, effectuer des calculs et réécrire sur S3
def preprocess_s3_data(S3_INPUT_KEY, S3_OUTPUT_KEY):
    s3_hook = S3Hook(aws_conn_id="aws_default")

    # 🔹 Lire le fichier directement depuis S3 sans le télécharger
    file_obj = s3_hook.get_key(S3_INPUT_KEY, bucket_name=S3_BUCKET)
    df = pd.read_csv(io.BytesIO(file_obj.get()["Body"].read()))  # Charger en mémoire
    print(f"--- Fichier lu depuis S3 : {S3_INPUT_KEY} ---")

    # Changer le nom de la première colonne en 'time'
    df.columns.values[0] = 'time'
    # Renommer les autres colonnes par les valeurs de la deuxième ligne de données
    new_column_names = ['time'] + df.iloc[1, 1:].tolist()
    df.columns = new_column_names
    # Supprimer la deuxième ligne de données car elle est maintenant utilisée comme en-tête
    df = df.drop(1).reset_index(drop=True)
    # extraire les metadatas
    metadatas = df.iloc[:4, :]
    # Enlever les lignes où 'time' est NaN & Enlever les lignes où les valeurs sont négatives
    df = df.dropna(subset=['time'])
    df = df[(df.select_dtypes(include=['number']) >= 0).all(axis=1)]

    # 🔹 Convertir en CSV et envoyer directement à S3
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_hook.load_string(csv_buffer.getvalue(), S3_OUTPUT_KEY, bucket_name=S3_BUCKET, replace=True)
    print(f"--- Résultats enregistrés sur S3 : s3://{S3_BUCKET}/{S3_OUTPUT_KEY} ---")


# S3_INPUT_KEY = "data_L0/department_map.zip"
# S3_OUTPUT_KEY = "data_L1/department_map.csv"
def preprocess_s3_map(S3_INPUT_KEY, S3_OUTPUT_KEY):
    import zipfile
    import geopandas as gpd
    s3_hook = S3Hook(aws_conn_id="aws_default")

    # Lire le fichier ZIP directement depuis S3 sans le télécharger
    file_obj = s3_hook.get_key(S3_INPUT_KEY, bucket_name=S3_BUCKET)
    zip_content = io.BytesIO(file_obj.get()["Body"].read())

    # Extraire le fichier shapefile du ZIP
    gdf = gpd.read_file(zip_content)
    # Convertir les géométries en WKT (Well-Known Text)
    gdf['geometry'] = gdf['geometry'].apply(lambda x: x.wkt)

    # Sauvegarder en CSV & envoyer sur S3
    csv_buffer = io.StringIO()
    gdf.to_csv(csv_buffer, index=False)
    s3_hook.load_string(csv_buffer.getvalue(), S3_OUTPUT_KEY, bucket_name=S3_BUCKET, replace=True)
    print(f"--- GeoDataFrame converti en CSV avec succès et sauvegardé sous le nom '{S3_OUTPUT_KEY}' ---")

def preprocess_s3_gps(S3_INPUT_KEY, S3_OUTPUT_KEY):
    import pandas as pd
    s3_hook = S3Hook(aws_conn_id="aws_default")

    # Lire le fichier ZIP directement depuis S3 sans le télécharger
    file_obj = s3_hook.get_key(S3_INPUT_KEY, bucket_name=S3_BUCKET)
    xls_content = io.BytesIO(file_obj.get()["Body"].read())

    xlsdata = pd.read_excel(xls_content)
    print(f"--- Fichier XLS lu avec succes depuis S3 : {S3_INPUT_KEY} ---")

    gps = xlsdata[['NatlStationCode', 'Name', 'Latitude', 'Longitude']]
    gps['GPS'] = gps[['Latitude', 'Longitude']].apply(tuple, axis=1)

    # Sauvegarder en CSV & envoyer sur S3
    csv_buffer = io.StringIO()
    gps.to_csv(csv_buffer, index=False)
    s3_hook.load_string(csv_buffer.getvalue(), S3_OUTPUT_KEY, bucket_name=S3_BUCKET, replace=True)
    print(f"--- XLS converti en CSV avec succès et sauvegardé sous le nom '{S3_OUTPUT_KEY}' ---")




#================================================================================================
# 📌 Fonction pour envoyer les données à Elasticsearch
def send_to_elasticsearch(S3_PARQUET_PATH, S3_BUCKET_NAME):
    # Connexion à Elasticsearch Cloud
    ES_CLOUD_ID = "Polluant_dashboard:dXMtZWFzdC0xLmF3cy5mb3VuZC5pbzo0NDMkYmEyZTg4ZWVkNTJlNDQ4MWEwY2E3YzE4ZWFkNDMxNzQkZDhkZjUxMWQ4Y2YzNGEwYWE0Zjk0MGJkZmFiN2E3N2Q="  # 📌 Trouvé dans Elastic Cloud
    ES_USERNAME = "elastic"
    ES_PASSWORD = "KBtXGBOpRFGI3oxWaeVlewbp"
    ES_INDEX = "pollution_data"
    es = Elasticsearch(
        cloud_id=ES_CLOUD_ID,
        basic_auth=(ES_USERNAME, ES_PASSWORD)
    )

    # Lire le fichier Parquet depuis S3 avec Pandas
    s3_client = boto3.client("s3")
    obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_PARQUET_PATH)
    df_pandas = pd.read_parquet(obj["Body"])

    # Indexer les documents dans Elasticsearch
    for _, row in df_pandas.iterrows():
        document = row.to_dict()
        es.index(index=ES_INDEX, document=document)

    print(f"✅ {len(df_pandas)} documents indexés dans Elasticsearch")

#================================================================================================
# Définition du DAG Airflow

with DAG(
    "api_to_s3_pipeline",
    schedule_interval='@hourly',
    start_date=days_ago(1),
    catchup=False #(ici, False signifie ne pas rattraper les exécutions manquées).
) as dag:
    # fetch_map = PythonOperator(
    #     task_id="extract_department_map",
    #     python_callable=fetch_store_data_to_s3,
    #     op_kwargs={
    #         "url": urls[1],
    #         "headers": None,
    #         "S3_FILE_PATH": 'data_L0/maps/department_map.zip'
    #     }
    # )
    fetch_gps = PythonOperator(
        task_id="extract_region_zas_gps",
        python_callable=fetch_store_data_to_s3,
        op_kwargs={
            "url": urls[-1],
            "headers": None,
            "S3_FILE_PATH": 'data_L0/maps/fr-2025-d-lcsqa-ineris-20250113.xls'
        }
    )
    fetch_realtime_data = PythonOperator(
        task_id="extract_realtime_data",
        python_callable=fetch_store_data_to_s3,
        op_kwargs={
            "url": f"{urls[2]}{year}/FR_E2_{current_date.strftime("%Y-%m-%d")}.csv",
            "headers": None,
            "S3_FILE_PATH": f'data_L0/FR_E2_{current_date.strftime("%Y-%m-%d")}.csv'
        }
    )
    preprocess_data = SparkSubmitOperator(
        task_id="formatted_data_spark",
        application="dags/spark_preprocess_realtime.py",  # 🔹 Chemin du script Spark
        conn_id="spark_connection",  # 🔹 Connexion Spark définie dans Airflow
        conf={
            "spark_binary": "/opt/anaconda3/bin/spark-submit",
            "spark.hadoop.fs.s3a.access.key": aws_access_id,
            "spark.hadoop.fs.s3a.secret.key": aws_access_secret,
            "spark.hadoop.fs.s3a.endpoint": "s3.us-east-1.amazonaws.com",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4"
        },
        dag=dag
    )
    formatted_map = PythonOperator(
        task_id="formatted_region_gps",
        python_callable=preprocess_s3_gps,
        op_kwargs={
            "S3_INPUT_KEY": "data_L0/maps/fr-2025-d-lcsqa-ineris-20250113.xls",
            "S3_OUTPUT_KEY": "data_L1/maps/zas_region_gps.csv"
        }
    )
    combine_data = SparkSubmitOperator(
        task_id="combine_datasets",
        application="dags/spark_combine_processing.py",  # Chemin du script Spark
        conn_id="spark_connection",  #Connexion Spark définie dans Airflow
        conf={
            "spark_binary": "/opt/anaconda3/bin/spark-submit",
            "spark.hadoop.fs.s3a.access.key": aws_access_id,
            "spark.hadoop.fs.s3a.secret.key": aws_access_secret,
            "spark.hadoop.fs.s3a.endpoint": "s3.us-east-1.amazonaws.com",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4"
        },
        dag=dag
    )
    index_data = SparkSubmitOperator(
        task_id="index_data_in_elasticsearch",
        application="dags/to_kibana.py",  # 🔹 Chemin du script Spark
        conn_id="spark_connection",  # 🔹 Connexion Spark définie dans Airflow
        conf={
            "spark_binary": "/opt/anaconda3/bin/spark-submit",
            "spark.hadoop.fs.s3a.access.key": aws_access_id,
            "spark.hadoop.fs.s3a.secret.key": aws_access_secret,
            "spark.hadoop.fs.s3a.endpoint": "s3.us-east-1.amazonaws.com",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4"
        },
        dag=dag
    )
    # Ordre d'exécution des tâches
    [fetch_gps >> formatted_map, fetch_realtime_data >> preprocess_data] >> combine_data >> index_data
