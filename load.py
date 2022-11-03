import requests
import pandas as pd
from google.cloud import bigquery
import datetime
from datetime import timezone

client = bigquery.Client()
table_id="voltaic-country-280607.IP_WeatherMap.Historical_Weather"
def weather():
    api_url = "http://api.openweathermap.org/data/2.5/forecast?id=1581130&lang=vi&units=metric&appid=07a92bdc234b052aee22ad1f7151f201"
    request = requests.get(api_url)
    request_json = request.json()
    lists = request_json["list"]    
    rows_to_insert=[]
    for list_ in lists:
        main = list_["main"]
        time = list_["dt_txt"]
        weathers = list_["weather"]
        weather = weathers[0]
        description = weather["description"]
        temp = main["temp"]
        temp_min = main["temp_min"]
        temp_max = main["temp_max"]
        batched_date = datetime.datetime.now(timezone.utc)
        my_list=(temp, temp_min, temp_max, description, time, batched_date)
        rows_to_insert.append(my_list)
    table = client.get_table(table_id)
    errors = client.insert_rows(table, rows_to_insert)
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))