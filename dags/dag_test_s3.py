from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
import pandas as pd
import io

# 📌 Paramètres
S3_BUCKET = "data-lake-telecom1"
S3_INPUT_KEY = "data_api/data.csv"  # Fichier source
S3_OUTPUT_KEY = "data_L1/formatted_data_2024_annual.csv"  # Résultat après calculs

# 📌 Fonction pour lire S3, effectuer des calculs et réécrire sur S3
def process_s3_data():
    s3_hook = S3Hook(aws_conn_id="aws_default")

    # 🔹 Lire le fichier directement depuis S3 sans le télécharger
    file_obj = s3_hook.get_key(S3_INPUT_KEY, bucket_name=S3_BUCKET)
    df = pd.read_csv(io.BytesIO(file_obj.get()["Body"].read()))  # Charger en mémoire

    # Changer le nom de la première colonne en 'time'
    df.columns.values[0] = 'time'
    # Renommer les autres colonnes par les valeurs de la deuxième ligne de données
    new_column_names = ['time'] + df.iloc[1, 1:].tolist()
    df.columns = new_column_names
    # Supprimer la deuxième ligne de données car elle est maintenant utilisée comme en-tête
    df = df.drop(1).reset_index(drop=True)
    # extraire les metadatas
    metadatas = df.iloc[:4, :]
    # Enlever les lignes où 'time' est NaN
    df = df.dropna(subset=['time'])
    # Enlever les lignes où les valeurs sont négatives
    df = df[(df.select_dtypes(include=['number']) >= 0).all(axis=1)]

    # 🔹 Convertir en CSV et envoyer directement à S3
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_hook.load_string(csv_buffer.getvalue(), S3_OUTPUT_KEY, bucket_name=S3_BUCKET, replace=True)

    print(f"✅ Résultats enregistrés sur S3 : s3://{S3_BUCKET}/{S3_OUTPUT_KEY}")

# 📌 Définition du DAG
with DAG("s3_processing_pipeline",
         schedule_interval="@daily",
         start_date=days_ago(1),
         catchup=False) as dag:

    process_task = PythonOperator(
        task_id="process_s3_data",
        python_callable=process_s3_data
    )
