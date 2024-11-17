import os
import sys
import time
import logging
import requests
from bs4 import BeautifulSoup
from logging.handlers import RotatingFileHandler
from database.influx import client, bucket, org, url
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS

link = {
    "MYR": "https://www.google.com/finance/quote/SGD-MYR?hl=en",
    "JPY": "https://www.google.com/finance/quote/SGD-JPY?hl=en",
    "CNY": "https://www.google.com/finance/quote/SGD-CNY?hl=en",
    "TWD": "https://www.google.com/finance/quote/SGD-TWD?hl=en",
    "GBP": "https://www.google.com/finance/quote/SGD-GBP?hl=en",
    "AED": "https://www.google.com/finance/quote/SGD-AED?hl=en"
}

def get_rate(url: str) -> float:
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    
    rate = soup.find("div", attrs={"class": "YMlKec fxKbKc"}).get_text()
    return float(rate)

if __name__ == "__main__":
    log_path = os.path.join(os.getcwd(), "log/crawler.log")
    
     # Formatter for stdout logger
    stdout_formatter = logging.Formatter(f"[%(levelname)s] - %(message)s")

    # Formatter for file logger with timestamp
    file_formatter = logging.Formatter(f"%(asctime)s %(pathname)s [%(levelname)s] - %(message)s",
                                    datefmt="%Y-%m-%d %H:%M:%S")

    # Create stdout handler with INFO level
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.setFormatter(stdout_formatter)

    # Create rotating file handler with WARN level, max size 10MB, and backup count of 10 files
    file_handler = RotatingFileHandler(log_path, maxBytes=10 * 1024 * 1024, backupCount=10)
    file_handler.setLevel(logging.WARN)
    file_handler.setFormatter(file_formatter)
    
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(stdout_handler)
    logger.addHandler(file_handler)
    
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    # Crawl about every 10 seconds for the currency rate
    while True:
        point_list = []
        for (target, url) in link.items():
            try:
                rate = get_rate(url)
                logger.info("SGD --> {target}: {rate:.4f}".format(target = target, rate = rate))
                point_list.append(Point("currency").tag("source", "SGD").tag("target", target).field("rate", rate))
            
            except AttributeError as error:
                # Query page failed
                # Skip the currency
                time.sleep(1)
                continue
            
        try:
            write_api.write(bucket = bucket, org = org, record = point_list)
            time.sleep(10)
        
        except Exception as error:
            # Lost connection to InfluxDB
            # Trigger service auto restart
            logger.error(error)
            time.sleep(3)
            sys.exit(1)