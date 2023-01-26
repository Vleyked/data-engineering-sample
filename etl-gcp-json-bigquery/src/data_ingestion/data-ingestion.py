import os
import json
import pandas as pd


def data_ingestion():
    """Read raw data files, perform data cleaning and validation, and save intermediate data files"""
    # Read raw data file1
    with open(os.path.join("data/raw/raw_data_folder", "raw_data_file1.json")) as file:
        data1 = json.load(file)

    # Read raw data file2
    with open(os.path.join("data/raw/raw_data_folder", "raw_data_file2.json")) as file:
        data2 = json.load(file)

    # Convert data to dataframes
    df1 = pd.DataFrame(data1)
    df2 = pd.DataFrame(data2)

    # Perform data cleaning and validation
    df1 = clean_validate_data(df1)
    df2 = clean_validate_data(df2)

    # Save intermediate data file1
    df1.to_json(
        os.path.join(
            "data/intermediate/intermediate_data_folder", "intermediate_data_file1.json"
        ),
        orient="records",
        lines=True,
    )

    # Save intermediate data file2
    df2.to_json(
        os.path.join(
            "data/intermediate/intermediate_data_folder", "intermediate_data_file2.json"
        ),
        orient="records",
        lines=True,
    )


def clean_validate_data(df):
    """parse and convert GPS coordinates to decimal degrees format

    Args:
        df (pandas.dataframe): Pandas dataframe ingested from raw data file

    Returns:
        pandas.dataframe: Pandas dataframe with cleaned and validated data
    """
    # Perform data cleaning and validation
    # Remove rows with missing values
    df = df.dropna()
    # Drop GPS points within the boundaries of Barcelona
    barcelona_boundaries = {"north": 41.5, "south": 41.2, "west": 2.0, "east": 2.2}

    df = df[
        (df["latitude"] < barcelona_boundaries["north"])
        & (df["latitude"] > barcelona_boundaries["south"])
        & (df["longitude"] > barcelona_boundaries["west"])
        & (df["longitude"] < barcelona_boundaries["east"])
    ]

    # Convert GPS coordinates to decimal degrees format
    df["latitude"] = df["latitude"].apply(
        lambda x: convert_to_decimal_degrees(x, "latitude")
    )
    df["longitude"] = df["longitude"].apply(
        lambda x: convert_to_decimal_degrees(x, "longitude")
    )

    return df


def convert_to_decimal_degrees(coord, coord_type):
    """Convert GPS coordinates to decimal degrees format

    Args:
        coord (int or float): GPS coordinates
        coord_type (str): GPS coordinates type

    Raises:
        ValueError: Invalid coord_type if coord_type is not "latitude" or "longitude"
        ValueError: Invalid coordinates format if coord is not in the expected format

    Returns:
        str: GPS coordinates in decimal degrees format
    """
    import re

    if coord_type not in ["latitude", "longitude"]:
        raise ValueError("Invalid coord_type")
    if re.match("^-?\d{1,3}\.\d+,-?\d{1,3}\.\d+$", coord):
        # Coordinates in decimal degrees format
        return coord
    elif re.match("^-?\d{1,3}\.\d+\s[NS],\s-?\d{1,3}\.\d+\s[EW]$", coord):
        # Coordinates in decimal minutes format
        coord = coord.split(",")
        lat = coord[0].strip()
        lon = coord[1].strip()
        lat_dir = lat[-1]
        lon_dir = lon[-1]
        lat = float(lat[:-1])
        lon = float(lon[:-1])
        lat = lat if lat_dir in ["N", "n"] else -lat
        lon = lon if lon_dir in ["E", "e"] else -lon
        return f"{lat},{lon}"
    elif re.match(
        "^-?\d{1,3}°\d{1,2}'\d{1,2}\.\d+\"[NS],\s-?\d{1,3}°\d{1,2}'\d{1,2}\.\d+\"[EW]$",
        coord,
    ):
        # Coordinates in degrees, minutes and seconds format
        coord = coord.split(",")
        lat = coord[0].strip()
        lon = coord[1].strip()
        lat_dir = lat[-1]
        lon_dir = lon[-1]
        lat = lat[:-1].split("°")
        lon = lon[:-1].split("°")
        lat_deg = float(lat[0])
        lon_deg = float(lon[0])
        lat_min = float(lat[1][:-1])
        lon_min = float(lon[1][:-1])
        lat_sec = float(lat[2][:-1])
        lon_sec = float(lon[2][:-1])
        lat = lat_deg + lat_min / 60 + lat_sec / 3600
        lon = lon_deg + lon_min / 60 + lon_sec / 3600
        lat = lat if lat_dir in ["N", "n"] else -lat
        lon = lon if lon_dir in ["E", "e"] else -lon
        return f"{lat},{lon}"
    else:
        raise ValueError("Invalid coordinates format")


if __name__ == "__main__":
    data_ingestion()
