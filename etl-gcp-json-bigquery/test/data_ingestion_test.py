import unittest
import os
from src.data_ingestion import ingest_data, convert_to_decimal_degrees


class TestDataIngestion(unittest.TestCase):
    def test_ingest_data(self):
        # Test input files
        raw_data_file1 = "data/raw/raw_data_folder/raw_data_file1.json"
        raw_data_file2 = "data/raw/raw_data_folder/raw_data_file2.json"

        # Test expected output files
        intermediate_data_file1 = (
            "data/intermediate/intermediate_data_folder/intermediate_data_file1.json"
        )
        intermediate_data_file2 = (
            "data/intermediate/intermediate_data_folder/intermediate_data_file2.json"
        )

        # Test if the function can correctly ingest data
        ingest_data(
            raw_data_file1,
            raw_data_file2,
            intermediate_data_file1,
            intermediate_data_file2,
        )
        self.assertTrue(os.path.exists(intermediate_data_file1))
        self.assertTrue(os.path.exists(intermediate_data_file2))

    def test_convert_to_decimal_degrees(self):
        # Test input coordinates in different
        coord1 = "40.785091, -73.968285"  # Decimal Degrees
        coord2 = "40° 47' 6.32752'' N, 73° 58' 5.82680'' W"  # Degrees, Minutes, Seconds
        coord3 = "40° 47.10546' N, 73° 58.09788' W"  # Degrees, Decimal Minutes
        coord4 = "40.785091 N, 73.968285 W"  # x y
        coord_type = "longitude"

        # Test expected output coordinates in decimal degrees
        expected_coord1 = "40.785091, -73.968285"
        expected_coord2 = "40.785091, -73.968285"
        expected_coord3 = "40.785091, -73.968285"
        expected_coord4 = "40.785091, -73.968285"

        # Test if the function can correctly convert the input coordinates to decimal
        self.assertEqual(
            convert_to_decimal_degrees(coord1, coord_type), expected_coord1
        )
        self.assertEqual(
            convert_to_decimal_degrees(coord2, coord_type), expected_coord2
        )
        self.assertEqual(
            convert_to_decimal_degrees(coord3, coord_type), expected_coord3
        )
        self.assertEqual(
            convert_to_decimal_degrees(coord4, coord_type), expected_coord4
        )


if __name__ == "__main__":
    unittest.main()
