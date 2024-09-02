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
    print(bucket)
    print(org)
    print(token)
    print(url)