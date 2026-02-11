# utils/config.py

import requests
import json
import psycopg2
import boto3
from psycopg2.extras import execute_values

# === Secrets Manager ===
def get_secret(secret_name, region_name='us-east-2'):
    client = boto3.client('secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

# === Load secrets ===
SECRET_NAME_DB = 'integration/admin'
SECRET_NAME_ZOHO = 'integration/ZohoAPI'
SECRET_NAME_SAP = 'integration/sapprd'

secret_db = get_secret(SECRET_NAME_DB)
secret_zoho = get_secret(SECRET_NAME_ZOHO)

# === Config values ===
ZOHO_ACCESS_TOKEN = secret_zoho['access_token']
PG_HOST = secret_db['host']
PG_DATABASE = secret_db['database_name']
PG_USER = secret_db['username']
PG_PASSWORD = secret_db['password']
PG_PORT = '5432'


def get_pg_credentials():
    """Returns PostgreSQL credentials from Secrets Manager."""
    creds = get_secret(SECRET_NAME_DB)
    return creds

def get_sap_credentials():
    """Returns SAP credentials from Secrets Manager."""
    creds = get_secret(SECRET_NAME_SAP)
    return creds

# === DB Connection ===
def get_pg_connection():
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT
    )
    return conn, conn.cursor()

# === ZOHO headers ===
def get_zoho_headers():
    return {
        'Authorization': f'Zoho-oauthtoken {ZOHO_ACCESS_TOKEN}'
    }

# === Paginated fetch for any Zoho module ===
def fetch_zoho_data(module_name: str):
    headers = get_zoho_headers()
    base_url = f'https://recruit.zoho.com/recruit/v2/{module_name}'
    page = 1
    all_records = []

    while True:
        params = {'page': page, 'per_page': 100}
        response = requests.get(base_url, headers=headers, params=params)

        if response.status_code != 200 or not response.text.strip():
            print(f"Stopping at page {page} — status {response.status_code}")
            break

        try:
            data = response.json()
        except json.JSONDecodeError:
            print(f"Invalid JSON on page {page}")
            break

        if 'data' not in data or not data['data']:
            break

        all_records.extend(data['data'])
        page += 1

    return all_records
