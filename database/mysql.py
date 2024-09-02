import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

def get_connection_pool():
    return mysql.connector.connect(
        host = os.getenv('MYSQL_URL'),
        user = os.getenv('MYSQL_USER'),
        password = os.getenv('MYSQL_PASSWORD'),
        database = os.getenv('MYSQL_DB')
    )

if __name__ == "__main__":
    print(os.getenv('MYSQL_URL'))
    print(os.getenv('MYSQL_USER'))
    print(os.getenv('MYSQL_PASSWORD'))
    print(os.getenv('MYSQL_DB'))