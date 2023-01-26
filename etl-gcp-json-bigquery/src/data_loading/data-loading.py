import json
from google.cloud import bigquery


def load_data_to_bigquery(processed_data_file1, processed_data_file2):
    """Load data into BigQuery table"""
    client = bigquery.Client()
    dataset_id = "my_dataset_id"
    table_id = "my_table_id"
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = client.get_table(table_ref)

    # Read processed data files
    with open(processed_data_file1, "r") as f:
        data1 = json.load(f)
    with open(processed_data_file2, "r") as f:
        data2 = json.load(f)

    # Concatenate data from both files
    data = data1 + data2

    # Insert data into BigQuery table
    errors = client.insert_rows(table, data)
    if errors == []:
        print("Data loaded successfully.")
    else:
        print("Errors occurred while loading data:")
        for error in errors:
            print(error)
