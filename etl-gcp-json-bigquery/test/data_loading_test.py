import unittest
import pandas as pd
from google.cloud import bigquery
from src.data_loading import get_data_from_bigquery


class TestDataLoading(unittest.TestCase):
    """This test checks that the data loaded to BigQuery
    is equal to the processed data in processed_data_file1.json
    and processed_data_file2.json by comparing the output of the
    function `get_data_from_bigquery()` with the `processed_data`
    variable. If the test passes, it means that the data has been
    loaded correctly to BigQuery.
    """

    def setUp(self):
        self.processed_data_file1 = "processed_data_file1.json"
        self.processed_data_file2 = "processed_data_file2.json"
        self.client = bigquery.Client()
        self.dataset_id = "test_dataset"
        self.table_id1 = "test_table1"
        self.table_id2 = "test_table2"
        self.table_ref1 = self.client.dataset(self.dataset_id).table(self.table_id1)
        self.table_ref2 = self.client.dataset(self.dataset_id).table(self.table_id2)

    def test_load_data(self):
        data1 = pd.read_json(self.processed_data_file1)
        data2 = pd.read_json(self.processed_data_file2)
        self.client.load_table_from_dataframe(data1, self.table_ref1).result()
        self.client.load_table_from_dataframe(data2, self.table_ref2).result()

        query = f"SELECT COUNT(*) as count FROM {self.dataset_id}.{self.table_id1}"
        query_job = self.client.query(query)
        result = query_job.to_dataframe()
        count1 = result["count"][0]
        self.assertEqual(count1, len(data1))

        query = f"SELECT COUNT(*) as count FROM {self.dataset_id}.{self.table_id2}"
        query_job = self.client.query(query)
        result = query_job.to_dataframe()
        count2 = result["count"][0]
        self.assertEqual(
            get_data_from_bigquery("project_id", "dataset_id", "table_id"),
            processed_data,
        )


if __name__ == "__main__":
    unittest.main()
