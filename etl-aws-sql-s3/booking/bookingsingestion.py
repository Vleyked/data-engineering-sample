import pandas as pd
import pandas.io.sql as psql
import boto3
import io
import logging
import pyarrow
from configuration import Configuration


class BookingsIngestion(Configuration):
    def __init__(self):
        super().__init__()

    def _get_last_datetime(self, df):
        df["created_on"] = pd.to_datetime(df.created_on)
        return (
            df.created_on.sort_values(ascending=True, ignore_index=True)
            .iloc[-1]
            .strftime("%F %H:%M:%S.%f%z")
        )

    def bookingsDf_full_s3Load(self):

        dft1 = psql.read_sql(
            """SELECT
                    vehicles.vehicle_id,
                    vehicles.vehicle_type,
                    COUNT(bookings.booking_id) AS total_bookings,
                    SUM(bookings.duration) AS total_duration,
                    AVG(bookings.duration) AS avg_duration,
                    MIN(bookings.start_time) AS earliest_booking,
                    MAX(bookings.end_time) AS latest_booking,
                    SUM(CASE WHEN bookings.is_cancelled THEN 1 ELSE 0 END) AS total_cancelled_bookings,
                    ROUND(SUM(CASE WHEN bookings.is_cancelled THEN 1 ELSE 0 END) / COUNT(bookings.booking_id) * 100, 2) AS cancellation_rate
                FROM
                    bookings
                JOIN
                    vehicles
                ON
                    bookings.vehicle_id = vehicles.vehicle_id
                WHERE
                    vehicles.vehicle_type = 'micromobility'
                GROUP BY
                    vehicles.vehicle_id,
                    vehicles.vehicle_type
                ORDER BY
                    total_bookings DESC
                ;""",
            self.conn,
        )

        parquet_buffer, bucket = io.BytesIO(), "{bucker_name}"

        dft1.to_parquet(parquet_buffer, compression="gzip")

        s3_resource = boto3.resource("s3")
        s3_resource.Object(bucket, f"xx/xx{self.today.year}.parquet").put(
            Body=parquet_buffer.getvalue()
        )

    def bookingsDf_delta_s3Load(self):

        bookings_data = self.s3c.get_object(Bucket=self.BUCKET_NAME, Key=self.BOOKINGS)

        bookings_data = pd.read_parquet(
            io.BytesIO(bookings_data["Body"].read()), engine="pyarrow"
        )

        current_last_datetime = self._get_last_datetime(bookings_data)

        del bookings_data

        dft1 = psql.read_sql(
            """SELECT
                    vehicles.vehicle_id,
                    vehicles.vehicle_type,
                    COUNT(bookings.booking_id) AS total_bookings,
                    SUM(bookings.duration) AS total_duration,
                    AVG(bookings.duration) AS avg_duration,
                    MIN(bookings.start_time) AS earliest_booking,
                    MAX(bookings.end_time) AS latest_booking,
                    SUM(CASE WHEN bookings.is_cancelled THEN 1 ELSE 0 END) AS total_cancelled_bookings,
                    ROUND(SUM(CASE WHEN bookings.is_cancelled THEN 1 ELSE 0 END) / COUNT(bookings.booking_id) * 100, 2) AS cancellation_rate
                FROM
                    bookings
                JOIN
                    vehicles
                ON
                    bookings.vehicle_id = vehicles.vehicle_id
                WHERE
                    vehicles.vehicle_type = 'micromobility'
                GROUP BY
                    vehicles.vehicle_id,
                    vehicles.vehicle_type
                ORDER BY
                    total_bookings DESC
                ;""",
            self.conn,
        )
        print(current_last_datetime)
        print(self._get_last_datetime(dft1))

        if current_last_datetime == self._get_last_datetime(dft1):
            logging.warning("Dataset is already up to date or replica is not updated.")
        else:
            parquet_buffer, bucket = io.BytesIO(), "generic-bucket"

            dft1.to_parquet(parquet_buffer, compression="gzip")

            s3_resource = boto3.resource("s3")
            s3_resource.Object(bucket, f"generic/generic{self.today.year}.parquet").put(
                Body=parquet_buffer.getvalue()
            )
