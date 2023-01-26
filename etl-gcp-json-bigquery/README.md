# ETL Project
This project is designed to extract, transform, and load data from raw data files to a processed format ready for analysis. The data is stored in three main folders: `raw`, `intermediate`, and `processed`.

## Data Ingestion
The first step in the ETL process is data ingestion. The `data_ingestion.py` script reads in the `raw` data files, located in the raw folder, and performs initial cleaning and validation on the data. This includes checking for missing or duplicate values, and converting GPS coordinates to decimal degrees format. The cleaned and validated data is then saved to the `intermediate` folder as `intermediate_data_file1.json` and `intermediate_data_file2.json`.

## Data Transformation
The next step in the ETL process is data transformation. The `data_transformation.py` script reads in the data from the `intermediate` folder and performs more complex transformations on the data. This includes dropping any values within the boundaries of the city of Barcelona, and aggregate data based on certain columns. The transformed data is then saved to the `processed` folder as `processed_data_file1.json` and `processed_data_file2.json`.

## Data Loading
The final step in the ETL process is data loading. The `data_loading.py` script reads in the data from the `processed` folder and loads it into BigQuery. The data is loaded into two separate tables, `processed_data_file1` and `processed_data_file2`.

## Conclusion

This ETL project demonstrates the process of extracting, transforming, and loading data from raw files to a processed format ready for analysis. By following best practices for data cleaning and validation, and performing complex transformations on the data, we can ensure that the data is accurate and ready for further analysis.

## How to Run
Install the required packages by running `pip install -r requirements.txt`
Run the ETL pipeline by running `python run_etl.py`
## Additional Resources
[Pandas documentation](https://pandas.pydata.org/docs/)
