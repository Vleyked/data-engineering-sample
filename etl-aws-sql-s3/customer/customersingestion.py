import pandas as pd
import pandas.io.sql as psql
import boto3
import io
import logging
import pyarrow
from configuration import Configuration


class CustomersIngestion(Configuration):
    def __init__(self):
        super().__init__()

    def customersDf_full_s3Load(self):

        dft1 = psql.read_sql(
            """SELECT
                u.id as user_id, u.locale, u.marketing_accepted, 
                u.privacy_accepted, u.created_on, u.updated_on,
                p.city, p.street, p.postal_code, p.status, p.birth_date, 
                p.card_status as credit_card_status
            FROM
                mobility.users.user u

            INNER JOIN  mobility.users.profile p

            ON u.id = p.user_id
            ;""",
            self.conn,
        )

        dft2 = psql.read_sql(
            """SELECT
                u.id as user_id, u.locale, u.marketing_accepted, 
                u.privacy_accepted, u.created_on, u.updated_on,
                p.city, p.street, p.postal_code, p.status, 
                p.birth_date, p.card_status as credit_card_status
            FROM
                mobility.users.deleted_user u

            INNER JOIN  mobility.users.deleted_profile p

            ON u.id = p.user_id
                    ;""",
            self.conn,
        )

        def _user_is_deleted(u_id):
            """Check wether user_id is a deleted user.

            Input:
            user_id STR: User id from user and deleted_user tables.

            Output:
            Bool: True if user is deleted, false otherwise.
            """
            deleted_users = dft2.user_id

            return u_id in deleted_users

        merged_dft = dft1.append(dft2)

        merged_dft["deleted_user"] = merged_dft["user_id"].apply(_user_is_deleted)

        # return merged_dft when the new function get_customer_df is refractored, I'll just do the full load just for now
        parquet_buffer = io.BytesIO()  # Add, getter for  referencing user id

        merged_dft.to_parquet(parquet_buffer, compression="gzip")

        s3_resource = boto3.resource("s3")
        s3_resource.Object(self.BUCKET_NAME, self.CUSTOMERS).put(
            Body=parquet_buffer.getvalue()
        )

        return None

    def customersDf_delta_s3Load(self):
        # TODO: Add a getter in the CONFIGURATION class mapping user_id and Key properties
        def _user_is_deleted(u_id):
            """
            Check wether user_id is a deleted user.

            Input:
            user_id STR: User id from user and deleted_user tables.

            Output:
            Bool: True if user is deleted, false otherwise.
            """
            deleted_users = dft2.user_id

            return u_id in deleted_users

        customers_data = self.s3c.get_object(
            Bucket=self.BUCKET_NAME, Key=self.CUSTOMERS
        )

        customers_data = pd.read_parquet(
            io.BytesIO(customers_data["Body"].read()), engine="pyarrow"
        )

        current_userIds = customers_data.user_id

        del customers_data

        dft1 = psql.read_sql(
            """WITH data_cte AS (
                SELECT 
                    user_id, 
                    event_date, 
                    event_type, 
                    event_amount,
                    DATE_TRUNC('month', event_date) AS month,
                    DATE_TRUNC('year', event_date) AS year
                FROM events
            )

            SELECT 
                user_id, 
                month,
                year,
                event_type, 
                SUM(event_amount) AS event_amount_sum,
                COUNT(DISTINCT event_date) AS event_count,
                AVG(event_amount) AS event_amount_avg
            FROM data_cte
            GROUP BY ROLLUP (user_id, month, year, event_type);
            """,
            self.conn,
        )

        dft2 = psql.read_sql(
            """WITH data_cte AS (
                SELECT 
                    user_id, 
                    event_date, 
                    event_type, 
                    CASE 
                        WHEN event_type = 'purchase' THEN event_amount
                        ELSE 0
                    END AS purchase_amount,
                    CASE 
                        WHEN event_type = 'refund' THEN event_amount
                        ELSE 0
                    END AS refund_amount
                FROM events
            )
            SELECT 
                user_id, 
                SUM(purchase_amount) - SUM(refund_amount) AS net_purchases,
                SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS total_purchases,
                SUM(CASE WHEN event_type = 'refund' THEN 1 ELSE 0 END) AS total_refunds
            FROM data_cte
            GROUP BY user_id
            """,
            self.conn,
        )

        merged_dft = dft1.append(dft2)

        merged_dft["deleted_user"] = merged_dft["user_id"].apply(_user_is_deleted)

        if set(current_userIds) == set(merged_dft.user_id):
            logging.warning(
                "Customera dataset is up to date or replica is not updated yed"
            )
        else:

            parquet_buffer = io.BytesIO()

            merged_dft.to_parquet(parquet_buffer, compression="gzip")

            s3_resource = boto3.resource("s3")
            s3_resource.Object(self.BUCKET_NAME, self.CUSTOMERS).put(
                Body=parquet_buffer.getvalue()
            )
