from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd

# URL de l'API Data.gouv.fr
API_URL = "https://tabular-api.data.gouv.fr/api/resources/1c5075ec-7ce1-49cb-ab89-94f507812daf/"

# Configuration du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_gouv_ingestion',
    default_args=default_args,
    description='Fetch data from Data.gouv.fr API and store it as CSV',
    schedule_interval=timedelta(days=1),  # Exécution quotidienne
)

def fetch_data():
    """Télécharge les données JSON depuis l'API et les enregistre en CSV"""
    response = requests.get(API_URL)

    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)  # Convertir JSON en DataFrame Pandas

        csv_path = "/tmp/data_gouv.csv"
        df.to_csv(csv_path, index=False)

        print(f"✅ Données enregistrées dans {csv_path}")
    else:
        raise Exception(f"❌ Erreur API : {response.status_code}")

fetch_task = PythonOperator(
    task_id='fetch_data_gouv',
    python_callable=fetch_data,
    dag=dag,
)

fetch_task
