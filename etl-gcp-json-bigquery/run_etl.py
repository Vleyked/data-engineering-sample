from src.data_ingestion import data_ingestion
from src.data_transformation import data_transformation
from src.data_loading import data_loading
from config import config


def run_etl():
    # Extract raw data
    raw_data1 = data_ingestion.ingest_raw_data(config.raw_data_file1)
    raw_data2 = data_ingestion.ingest_raw_data(config.raw_data_file2)

    # Transform data
    intermediate_data1 = data_transformation.transform_data(raw_data1)
    intermediate_data2 = data_transformation.transform_data(raw_data2)

    # Load data to BigQuery
    data_loading.load_data_to_bigquery(intermediate_data1, config.bigquery_table_name_1)
    data_loading.load_data_to_bigquery(intermediate_data2, config.bigquery_table_name_2)


if __name__ == "__main__":
    run_etl()
