import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# Take all credentials data from .env
cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
credentials = service_account.Credentials.from_service_account_file(cred_path)
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = "" ##input your desired table's name here
DATA_DIR = "/opt/airflow/data"

# initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

def create_dataset():
    """Create dataset if not exist yet"""
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "asia-southeast2"
    dataset_ref._properties["datasetReference"]["projectId"] = PROJECT_ID

    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {DATASET_ID} already exists")
    except Exception:
        client.create_dataset(dataset_ref)
        print(f"Dataset {DATASET_ID} created")

def truncate_dataset():
    """Truncate all table in schema"""
    print(f"Delete all table in dataset: {DATASET_ID} ...")
    tables = client.list_tables(DATASET_ID)
    for table in tables:
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table.table_id}"
        client.delete_table(table_id, not_found_ok=True)
        print(f"Deleted table: {table_id}")
    print("All table deleted")

def upload_csv(file, table):
    """Upload CSV to BigQuery table"""
    df = pd.read_csv(os.path.join(DATA_DIR, file))
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Uploaded {file} → {table_id}")

if __name__ == "__main__":
    create_dataset()
    truncate_dataset()  #Truncate all table before inserting
    files = {
        "colors.csv": "colors",
        "inventories.csv": "inventories",
        "inventory_parts.csv": "inventory_parts",
        "inventory_sets.csv": "inventory_sets",
        "sets.csv": "sets",
        "themes.csv": "themes",
        "parts.csv": "parts",
        "part_categories.csv": "part_categories"
    }
    for file, table in files.items():
        upload_csv(file, table)
