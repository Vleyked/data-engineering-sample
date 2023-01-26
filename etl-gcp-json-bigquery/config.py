class Config:
    def __init__(self):
        self.raw_data_folder = "data/raw/raw_data_folder"
        self.intermediate_data_folder = "data/intermediate/intermediate_data_folder"
        self.processed_data_folder = "data/processed/processed_data_folder"
        self.bigquery_project_id = "my-project-id"
        self.bigquery_dataset_id = "my-dataset-id"
        self.bigquery_table_name_1 = "processed_data_file1"
        self.bigquery_table_name_2 = "processed_data_file2"
