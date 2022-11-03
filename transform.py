import requests
import pandas as pd
from google.cloud import bigquery
import datetime
from datetime import timezone

client = bigquery.Client()
table_id="IP_WeatherMap.Historical_Weather"

def _update_weather():
    table = client.get_table(table_id)
    client.query(
        f"""
        CREATE OR REPLACE TABLE IP_WeatherMap.Historical_Weather AS
        SELECT * EXCEPT (row_num)
            FROM
            (
               SELECT
                *,
                ROW_NUMBER() over (
                    PARTITION BY time
                ) AS row_num
            FROM `IP_WeatherMap.Historical_Weather`
            ) 
            where row_num=1
        """).result()