import psycopg2
import boto3


class Configuration(object):
    def __init__(self):
        import os

        self.conn = psycopg2.connect(
            user="",
            password="",
            host="",
            port="",
            database="",
        )

        self.REGION = ""

        self.ACCESS_KEY_ID = ""

        self.SECRET_ACCESS_KEY = ""

        self.s3c = boto3.client(
            "s3",
            region_name=self.REGION,
            aws_access_key_id=self.ACCESS_KEY_ID,
            aws_secret_access_key=self.SECRET_ACCESS_KEY,
        )

        # self.cursor = conn.cursor() # Only necessary if creating SQL tables

        self.today = datetime.date.today()

        # Constants
        self.BUCKET_NAME = "generic-bucket"
        self.TRIPS = f"generic/generic{self.today.year}.parquet"
        self.PARKINGS = f"generic/generic{self.today.year}.parquet"
        self.VEVENTS = f"generic/generic{self.today.year}.parquet"
        self.VEHICLES = f"generic/generic{self.today.year}.parquet"
        self.CUSTOMERS = f"generic/generic{self.today.year}.parquet"
        self.BOOKINGS = f"generic/generic{self.today.year}.parquet"

        os.environ["generic"] = self.ACCESS_KEY_ID
        os.environ["generic"] = self.SECRET_ACCESS_KEY
