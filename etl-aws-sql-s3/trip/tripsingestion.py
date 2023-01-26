import pandas as pd
import pandas.io.sql as psql
import boto3
import io
import logging
import pyarrow
from configuration import Configuration


class TripsIngestion(Configuration):
    def __init__(self):
        super().__init__()

    def _get_last_datetime(self, trips_dataset):
        return (
            trips_dataset.start_trip_date.sort_values(ascending=True, ignore_index=True)
            .iloc[-1]
            .strftime("%F %H:%M:%S.%f%z")
        )

    def tripsDf_full_s3Load(self):
        df1 = psql.read_sql(
            """WITH geospatial_data AS (
                    SELECT 
                        id,
                        ST_GeomFromText(geometry) AS geometry,
                        properties
                    FROM 
                        raw_data
                ),

                aggregated_data AS (
                    SELECT 
                        ST_Centroid(geometry) AS centroid,
                        ST_Area(geometry) AS area,
                        ST_Length(geometry) AS perimeter,
                        COUNT(*) as count
                    FROM 
                        geospatial_data
                    GROUP BY 
                        centroid,
                        area,
                        perimeter
                )

                SELECT 
                    id,
                    centroid,
                    area,
                    perimeter,
                    count,
                    properties
                FROM 
                    aggregated_data
                JOIN 
                    geospatial_data
                ON 
                    geospatial_data.id = aggregated_data.id;""",
            self.conn,
        )

        df2 = psql.read_sql(
            """WITH geospatial_data AS (
                    SELECT 
                        id, 
                        location, 
                        ST_Distance(location, ST_MakePoint(longitude, latitude)) as distance 
                    FROM 
                        table_name
                    WHERE 
                        ST_DWithin(location, ST_MakePoint(longitude, latitude), radius)
                )

                SELECT 
                    id, 
                    location, 
                    distance, 
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY distance) as rank 
                FROM 
                    generic-geospatial_data
                WHERE 
                    rank = 1;""",
            self.conn,
        )

        df3 = psql.read_sql(
            """WITH data_cte AS (
                    SELECT 
                        user_id, 
                        event_date, 
                        event_type, 
                        ROW_NUMBER() OVER (PARTITION BY user_id, event_type ORDER BY event_date) AS event_num
                    FROM generic-events
                )

                SELECT 
                    user_id, 
                    event_type, 
                    event_date, 
                    LAG(event_date, 1) OVER (PARTITION BY user_id, event_type ORDER BY event_date) AS prev_event_date,
                    DATEDIFF(day, LAG(event_date, 1) OVER (PARTITION BY user_id, event_type ORDER BY event_date), event_date) AS days_since_prev_event
                FROM generic-data_cte
                WHERE event_num = 1;""",
            self.conn,
        )

        df1.set_index("vehicle_trip_id", inplace=True)
        df2.set_index("vehicle_trip_id2", inplace=True)
        df3.set_index("vehicle_trip_id3", inplace=True)

        df = pd.concat([df1, df2, df3], axis=1, join="inner")
        df.index.set_names("vehicle_trip_id", inplace=True)

        parquet_buffer, bucket = io.BytesIO(), "generic-bucket"

        df.to_parquet(parquet_buffer, compression="gzip")

        s3_resource = boto3.resource("s3")
        s3_resource.Object(bucket, f"generic/generic{self.today.year}.parquet").put(
            Body=parquet_buffer.getvalue()
        )

    def tripsDf_delta_s3Load(self):

        trips_data = self.s3c.get_object(Bucket=self.BUCKET_NAME, Key=self.TRIPS)

        trips_data = pd.read_parquet(
            io.BytesIO(trips_data["Body"].read()), engine="pyarrow"
        )

        current_last_datetime = self._get_last_datetime(trips_data)

        del trips_data

        delta1 = psql.read_sql(
            f"""SELECT DISTINCT ON (vehicle_trip_id) created_on as start_trip_date, 
                                            vehicle_trip_id, 
                                            gps_lat as start_gps_lat,
                                            gps_lng as start_gps_lng

                                    FROM mobility.opendata.vehicle_trip_route_point

                                    WHERE created_on > {current_last_datetime}

                                    ORDER BY vehicle_trip_id, created_on asc
                                    ;""",
            self.conn,
        )

        delta2 = psql.read_sql(
            """SELECT DISTINCT ON (vehicle_trip_id) created_on as end_trip_date, 
                                            vehicle_trip_id as vehicle_trip_id2, 
                                            gps_lat as end_gps_lat, 
                                            gps_lng as end_gps_lng

                                    FROM mobility.opendata.vehicle_trip_route_point

                                    WHERE created_on > {current_last_datetime}

                                    ORDER BY vehicle_trip_id, created_on desc
                                ;""",
            self.conn,
        )

        delta3 = psql.read_sql(
            """SELECT id as vehicle_trip_id3, vehicle_type,
                                            vehicle_id, provider_id, propulsion_type,
                                            trip_duration_seconds, trip_distance_meters,
                                            end_time, initial_odometer_km,
                                            current_odometer_km, trip_id

                                    FROM mobility.opendata.vehicle_trip

                                    WHERE start_time > {current_last_datetime}

                                    ORDER BY id
                                ;""",
            self.conn,
        )

        delta1.set_index("vehicle_trip_id", inplace=True)
        delta2.set_index("vehicle_trip_id2", inplace=True)
        delta3.set_index("vehicle_trip_id3", inplace=True)

        deltadf = pd.concat([delta1, delta2, delta3], axis=1, join="inner")
        del delta1
        del delta2
        del delta3

        if current_last_datetime == self._get_last_datetime(deltadf):
            logging.warning(
                f"db has not been replicated yet or there is no delta rows since {current_last_datetime}"
            )

        else:
            deltadf.index.set_names("vehicle_trip_id", inplace=True)

            parquet_buffer, bucket = io.BytesIO(), "generic-bucket"

            deltadf.to_parquet(parquet_buffer, compression="gzip")

            s3_resource = boto3.resource("s3")
            s3_resource.Object(bucket, f"generic/generic{self.today.year}.parquet").put(
                Body=parquet_buffer.getvalue()
            )
