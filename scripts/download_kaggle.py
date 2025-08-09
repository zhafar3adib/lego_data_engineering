import os
from kaggle.api.kaggle_api_extended import KaggleApi

dataset_slug = "rtatman/lego-database"
download_dir = "/opt/airflow/data"
expected_files = ["colors.csv", "inventories.csv", "inventory_parts.csv", "inventory_sets.csv", "sets.csv", "themes.csv",
                  "parts.csv","part_categories.csv"]

def all_files_exist():
    return all(os.path.exists(os.path.join(download_dir, f)) for f in expected_files)

def download_if_needed():
    os.makedirs(download_dir, exist_ok=True)
    if all_files_exist():
        print("File available, skip download.")
        return
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset_slug, path=download_dir, unzip=True)
    print("Download process done")

if __name__ == "__main__":
    download_if_needed()
