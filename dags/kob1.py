from datetime import datetime, timedelta
import requests
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from utils.helpers import replace_table


# =========================
# CONFIGURATION
# =========================
TABLE_NAME = 'kob1'
SCHEMA = 'sap'
ENDPOINT = "http://10.0.2.17:8000/sap/bc/ZCLKOB1API"


# =========================
# MAIN ETL FUNCTION
# =========================
def fetch_and_load_kob1(**context):

    params = {
        "page": 1,
        "page_size": 1000
    }

    all_data = []

    while True:
        response = requests.get(
            ENDPOINT,
            params=params,
            auth=("m.wael", "Claw2001!!"),
            verify=False
        )

        if response.status_code != 200:
            raise Exception(f"Error fetching data: {response.status_code}")

        data = response.json()
        records = data.get('DATA', [])

        print(f"Fetched page {params['page']} with {len(records)} records")

        if not records:
            break

        all_data.extend(records)
        params['page'] += 1

    if not all_data:
        print("No data returned from API")
        return

    df = pd.DataFrame(all_data)
    df.columns = [col.lower() for col in df.columns]

    # Replace table in Postgres
    replace_table(
        "integration",
        "iYc-R-!865Mp",
        "18.216.3.24",
        '5432',
        "postgres",
        SCHEMA,
        df,
        TABLE_NAME
    )

    print("Data successfully loaded into Postgres")


# =========================
# DAG DEFINITION
# =========================
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='sap_kob1_etl',
    default_args=default_args,
    description='Fetch KOB1 data from SAP API and load to Postgres',
    schedule_interval='*/30 * * * *', 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sap', 'kob1', 'etl'],
) as dag:

    run_etl = PythonOperator(
        task_id='fetch_and_load_kob1',
        python_callable=fetch_and_load_kob1,
        provide_context=True,
    )

    run_etl
