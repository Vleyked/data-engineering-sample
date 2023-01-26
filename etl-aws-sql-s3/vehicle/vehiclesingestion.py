import pandas as pd
import pandas.io.sql as psql
import boto3
import io
import logging
import pyarrow
from datetime import date, timedelta
from configuration import Configuration


class VehiclesIngestion(Configuration):
    def __init__(self):
        super().__init__()
        self.yesterday = date.today() - timedelta(days=1)
        self.today = date.today()

    def _get_last_datetime(self, vehicles_dataset):
        return (
            vehicles_dataset.created_on.sort_values(ascending=True, ignore_index=True)
            .iloc[-1]
            .strftime("%F %H:%M:%S.%f%z")
        )

    def parkingAreasDf_full_s3Load(self):
        dft1 = psql.read_sql(
            """SELECT *
                FROM generic.parking_area
                ;""",
            self.conn,
        )

        parquet_buffer, bucket = io.BytesIO(), "generic-name"

        dft1.to_parquet(parquet_buffer, compression="gzip")

        s3_resource = boto3.resource("s3")
        s3_resource.Object(bucket, self.PARKINGS).put(Body=parquet_buffer.getvalue())

    def parkingAreasDf_delta_s3Load(self):
        parkings_data = self.s3c.get_object(Bucket=self.BUCKET_NAME, Key=self.PARKINGS)

        parkings_data = pd.read_parquet(
            io.BytesIO(parkings_data["Body"].read()), engine="pyarrow"
        )

        dft1 = psql.read_sql(
            f"""WITH parking_cte AS (
                SELECT 
                    area_id,
                    SUM(CASE WHEN status = 'occupied' THEN 1 ELSE 0 END) AS occupied_spaces,
                    SUM(CASE WHEN status = 'available' THEN 1 ELSE 0 END) AS available_spaces,
                    SUM(CASE WHEN status = 'reserved' THEN 1 ELSE 0 END) AS reserved_spaces,
                    COUNT(*) AS total_spaces
                FROM generic-parking-areas
                WHERE created_on <= '{self.today.strftime("%F %H:%M:%S.%f%z")}'
                AND created_on >= '{self.yesterday.strftime("%F %H:%M:%S.%f%z")}'
                GROUP BY area_id
            )

            SELECT 
                area_id,
                occupied_spaces,
                available_spaces,
                reserved_spaces,
                total_spaces,
                ROUND(occupied_spaces / total_spaces::float, 2) AS occupancy_rate,
                ROUND((available_spaces + reserved_spaces) / total_spaces::float, 2) AS availability_rate
            FROM generic-parking
            ORDER BY occupancy_rate DESC
            ;""",
            self.conn,
        )

        if len(set(parkings_data.id)) == len(set(dft1.id)):
            logging.warning(
                "Parking Area dataset is up to date or replica is not updated"
            )
            return None
        else:
            parquet_buffer, bucket = io.BytesIO(), "generic-name"

            dft1.to_parquet(parquet_buffer, compression="gzip")

            s3_resource = boto3.resource("s3")
            s3_resource.Object(bucket, self.PARKINGS).put(
                Body=parquet_buffer.getvalue()
            )
            return None

    def vehicleEventsDf_full_s3Load(self):
        dft1 = psql.read_sql(
            """SELECT 
            vehicle_id, 
            event_time, 
            event_type, 
            event_location,
            EXTRACT(MONTH FROM event_time) AS event_month,
            EXTRACT(YEAR FROM event_time) AS event_year,
            ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY event_time) AS event_num
            FROM generic-vehicle-events;""",
            self.conn,
        )

        parquet_buffer, bucket = io.BytesIO(), "generic-name"

        dft1.to_parquet(parquet_buffer, compression="gzip")

        s3_resource = boto3.resource("s3")
        s3_resource.Object(bucket, self.VEVENTS).put(Body=parquet_buffer.getvalue())

    def vehicleEventsDf_delta_s3Load(self):
        vEvents_data = self.s3c.get_object(Bucket=self.BUCKET_NAME, Key=self.VEVENTS)

        vEvents_data = pd.read_parquet(
            io.BytesIO(vEvents_data["Body"].read()), engine="pyarrow"
        )

        current_last_datetime = self._get_last_datetime(vEvents_data)

        del vEvents_data

        dft1 = psql.read_sql(
            f"""WITH data_cte AS (
                SELECT 
                    vehicle_id, 
                    event_time, 
                    event_type, 
                    event_location,
                    EXTRACT(MONTH FROM event_time) AS event_month,
                    EXTRACT(YEAR FROM event_time) AS event_year,
                    ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY event_time) AS event_num
                FROM generic-vehicle-events
                WHERE created_on <= '{self.today.strftime("%F %H:%M:%S.%f%z")}'
                AND created_on >= '{self.yesterday.strftime("%F %H:%M:%S.%f%z")}'
            ) 

            SELECT 
                vehicle_id, 
                event_type, 
                event_location,
                event_month,
                event_year,
                COUNT(DISTINCT event_num) AS total_events,
                COUNT(DISTINCT event_location) AS unique_locations,
                COUNT(CASE WHEN event_type = 'maintenance' THEN 1 ELSE NULL END) AS maintenance_events,
                AVG(TIMESTAMPDIFF(SECOND, LAG(event_time) OVER (PARTITION BY vehicle_id ORDER BY event_time), event_time)) AS avg_time_between_events
            FROM data_cte
            GROUP BY vehicle_id, event_month, event_year
            ;""",
            self.conn,
        )

        if current_last_datetime == self._get_last_datetime(dft1):
            logging.warning(
                f"db has not been replicated yet or there is no delta rows since {current_last_datetime}"
            )

        else:
            parquet_buffer, bucket = io.BytesIO(), "generic-name"

            dft1.to_parquet(parquet_buffer, compression="gzip")

            s3_resource = boto3.resource("s3")
            s3_resource.Object(bucket, self.VEVENTS).put(Body=parquet_buffer.getvalue())

    def vehiclesDf_full_s3Load(self):
        dft1 = psql.read_sql(
            """WITH data_cte AS (
                SELECT 
                    motorcycle_id, 
                    purchase_date, 
                    warranty_expiration_date,
                    DATEDIFF(day, purchase_date, warranty_expiration_date) AS warranty_duration,
                    ROUND(DATEDIFF(day, purchase_date, NOW())/365.25, 2) AS age_in_years,
                    (CASE 
                        WHEN age_in_years > warranty_duration THEN 'out of warranty'
                        WHEN age_in_years <= warranty_duration THEN 'in warranty'
                        ELSE 'N/A'
                    END) AS warranty_status
                FROM generic-vehicles
            )

            SELECT 
                motorcycle_id, 
                purchase_date, 
                warranty_expiration_date,
                warranty_duration,
                age_in_years,
                warranty_status,
                COUNT(*) OVER (PARTITION BY warranty_status) AS total_by_warranty_status
            FROM data_cte
            ORDER BY age_in_years DESC
            ;""",
            self.conn,
        )

        parquet_buffer, bucket = io.BytesIO(), "generic-name"

        dft1.to_parquet(parquet_buffer, compression="gzip")

        s3_resource = boto3.resource("s3")
        s3_resource.Object(bucket, self.VEHICLES).put(Body=parquet_buffer.getvalue())

    def vehiclesDf_delta_s3Load(self):
        vehicles_data = self.s3c.get_object(Bucket=self.BUCKET_NAME, Key=self.VEHICLES)

        vehicles_data = pd.read_parquet(
            io.BytesIO(vehicles_data["Body"].read()), engine="pyarrow"
        )

        current_vehicle_ids = set(vehicles_data.vehicle_id)

        del vehicles_data

        dft1 = psql.read_sql(
            f"""WITH data_cte AS (
                SELECT 
                    motorcycle_id, 
                    purchase_date, 
                    warranty_expiration_date,
                    DATEDIFF(day, purchase_date, warranty_expiration_date) AS warranty_duration,
                    ROUND(DATEDIFF(day, purchase_date, NOW())/365.25, 2) AS age_in_years,
                    (CASE 
                        WHEN age_in_years > warranty_duration THEN 'out of warranty'
                        WHEN age_in_years <= warranty_duration THEN 'in warranty'
                        ELSE 'N/A'
                    END) AS warranty_status
                FROM generic-vehicles
                WHERE created_on <= '{self.today.strftime("%F %H:%M:%S.%f%z")}'
                AND created_on >= '{self.yesterday.strftime("%F %H:%M:%S.%f%z")}'
            )

            SELECT 
                motorcycle_id, 
                purchase_date, 
                warranty_expiration_date,
                warranty_duration,
                age_in_years,
                warranty_status,
                COUNT(*) OVER (PARTITION BY warranty_status) AS total_by_warranty_status
            FROM data_cte
            ORDER BY age_in_years DESC
            ;""",
            self.conn,
        )

        if set(dft1.vehicle_id) == current_vehicle_ids:
            logging.warning(
                f" db has not been replicated yet or there is no new vehicles current number of vehicles: {len(current_vehicle_ids)}"
            )
        else:
            parquet_buffer, bucket = io.BytesIO(), "generic-name"

            dft1.to_parquet(parquet_buffer, compression="gzip")

            s3_resource = boto3.resource("s3")
            s3_resource.Object(bucket, self.VEHICLES).put(
                Body=parquet_buffer.getvalue()
            )
