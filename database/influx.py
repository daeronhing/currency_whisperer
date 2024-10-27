import os
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient

load_dotenv()

bucket = os.getenv('INFLUX_BUCKET')
org = os.getenv('INFLUX_ORG')
token = os.getenv('INFLUX_TOKEN')
url = os.getenv('INFLUX_URL')

client = InfluxDBClient(
        url=url,
        token=token,
        org=org
    )

if __name__ == "__main__":
    # import numpy as np
    # import datetime
    
    # print(bucket)
    # print(org)
    # print(token)
    # print(url)
    
    # query_api = client.query_api()
    
    # today_date = datetime.date.today()
    # yesterday_date = today_date + datetime.timedelta(days=-1)
    
    # start_time = datetime.datetime(
    #     year = yesterday_date.year,
    #     month = yesterday_date.month,
    #     day = yesterday_date.day
    # ).astimezone(datetime.timezone.utc).isoformat()
    
    # stop_time = datetime.datetime(
    #     year = today_date.year,
    #     month = today_date.month,
    #     day = today_date.day
    # ).astimezone(datetime.timezone.utc).isoformat()
    
    # query = 'from(bucket:"{bucket}")\
    #     |> range(start: {start_time}, stop: {stop_time})\
    #     |> filter(fn: (r) => r._measurement == "currency")\
    #     |> filter(fn: (r) => r.source == "{source}")\
    #     |> filter(fn: (r) => r.target == "{target}")\
    #     |> mean()'.format(
    #         bucket = bucket,
    #         start_time = start_time,
    #         stop_time = stop_time,
    #         source = "SGD",
    #         target = "JPY"
    #     )
        
    # table = query_api.query(org=org, query=query)
    # result = table.to_values(columns=['_value'])
    
    # print(result)   
    
    pass