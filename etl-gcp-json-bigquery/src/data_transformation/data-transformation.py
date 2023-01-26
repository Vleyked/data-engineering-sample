import re


def clean_validate_data(dataframe):
    # Drop any rows with missing or invalid values
    dataframe = dataframe.dropna(subset=["gps_coordinates"])
    dataframe = dataframe[
        dataframe["gps_coordinates"].apply(
            lambda x: re.match("^-?\d{1,3}\.\d+,-?\d{1,3}\.\d+$", x)
            or re.match("^-?\d{1,3}\.\d+\s[NS],\s-?\d{1,3}\.\d+\s[EW]$", x)
            or re.match(
                "^-?\d{1,3}\.\d+\s[NS],\s-?\d{1,3}\.\d+\s[EW]\s-?\d{1,2}\.\d+'\s-?\d{1,2}\.\d+\"$",
                x,
            )
        )
    ]
    dataframe = dataframe[
        dataframe.apply(
            lambda x: x["gps_coordinates"].split(",")[0] <= "90"
            and x["gps_coordinates"].split(",")[0] >= "-90"
            and x["gps_coordinates"].split(",")[1] <= "180"
            and x["gps_coordinates"].split(",")[1] >= "-180",
            axis=1,
        )
    ]

    return dataframe


def aggregate_data(dataframe):
    """Aggregate data by vehicle_id and year

    Args:
        dataframe (pandas.dataframe): Pandas dataframe with cleaned and validated data

    Returns:
        pandas.dataframe: Pandas dataframe with aggregated data
    """
    dataframe = (
        dataframe.groupby(["vehicle_id", "year"])["distance_traveled"]
        .sum()
        .reset_index()
    )
    return dataframe


def transform_data(dataframe):
    """Apply any desired data transformations

    Args:
        dataframe (pandas.dataframe): Pandas dataframe with aggregated data

    Returns:
        pandas.dataframe: Pandas dataframe with transformed data
    """
    # Apply any desired data transformations
    dataframe["distance_traveled"] = dataframe["distance_traveled"] * 0.621371
    return dataframe


def process_data(dataframe):
    dataframe = clean_validate_data(dataframe)
    dataframe = aggregate_data(dataframe)
    dataframe = transform_data(dataframe)
    return dataframe
