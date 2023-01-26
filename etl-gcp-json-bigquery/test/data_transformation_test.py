import unittest
import pandas as pd
from src.data_transformation import (
    clean_validate_data,
    convert_to_decimal_degrees,
    translate_gps_conventions,
)


class TestDataTransformation(unittest.TestCase):
    """This test file tests all the functions
    inside the data_transformation.py file.

    test_clean_validate_data() checks
    that the cleaned data has no missing
    values and that the number of rows
    is less than or equal to the raw data.

    test_convert_to_decimal_degrees() checks
    that the coordinates are correctly
    converted to decimal degrees.

    test_translate_gps_conventions() checks
    that the gps conventions are correctly.
    """

    def test_clean_validate_data(self):
        raw_data = pd.read_json("raw_data_file1.json")
        cleaned_data = clean_validate_data(raw_data)
        self.assertEqual(cleaned_data.isnull().sum().sum(), 0)
        self.assertTrue(cleaned_data.shape[0] <= raw_data.shape[0])

    def test_convert_to_decimal_degrees(self):
        coord = "-12.3456 N, 178.1234 W"
        coord_type = "latitude"
        self.assertEqual(convert_to_decimal_degrees(coord, coord_type), "-12.3456")
        coord_type = "longitude"
        self.assertEqual(convert_to_decimal_degrees(coord, coord_type), "-178.1234")

    def test_translate_gps_conventions(self):
        coord = "12degrees 34minutes 56seconds N, 78degrees 12minutes 34seconds W"
        self.assertEqual(translate_gps_conventions(coord), "-12.582222,-78.209444")

    def test_drop_barcelona_boundaries(self):
        data = pd.read_json("raw_data_file2.json")
        self.assertTrue(
            data[
                (data["latitude"] < 41.387917)
                | (data["latitude"] > 41.483589)
                | (data["longitude"] < 2.169919)
                | (data["longitude"] > 2.284420)
            ].empty
        )


if __name__ == "__main__":
    unittest.main()
