import os
import sys
import logging
import threading
from logging.handlers import RotatingFileHandler

import json
import requests
from bs4 import BeautifulSoup

import time
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS

import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from firebase_admin.exceptions import FirebaseError

load_dotenv()

bucket = os.getenv('INFLUX_BUCKET')
org = os.getenv('INFLUX_ORG')
token = os.getenv('INFLUX_TOKEN')
url = os.getenv('INFLUX_URL')

influx_client = InfluxDBClient(url=url, token=token, org=org)

wise_id = 39
wise_base_api = "https://api.transferwise.com/v3/comparisons/"

website_url = {
    "GoogleFinance": {
        "MYR": "https://www.google.com/finance/quote/SGD-MYR?hl=en",
        "JPY": "https://www.google.com/finance/quote/SGD-JPY?hl=en",
        "CNY": "https://www.google.com/finance/quote/SGD-CNY?hl=en",
        "TWD": "https://www.google.com/finance/quote/SGD-TWD?hl=en",
        "GBP": "https://www.google.com/finance/quote/SGD-GBP?hl=en",
        "AED": "https://www.google.com/finance/quote/SGD-AED?hl=en"
    },
    "Wise": {
        "MYR": "MYR",
        "AED": "AED",
        "AUD": "AUD",
        "CNY": "CNY",
        "EUR": "EUR",
        "GBP": "GBP",
        "JPY": "JPY",
        "KRW": "KRW",
    }
}

firebase_cred = credentials.Certificate('cred.json')
firebase_admin.initialize_app(firebase_cred, {
    'databaseURL': 'https://currency-whisperer-default-rtdb.asia-southeast1.firebasedatabase.app/'
})
ref = db.reference('online-banking/from-sgd')

def get_rate_from_google_finance(currency: str, url: str, update_dict: dict):
    try:
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
    
        rate = soup.find("div", attrs={"class": "YMlKec fxKbKc"}).get_text()
        logging.info("(GoogleFinance) SGD --> {currency}: {rate}".format(currency = currency, rate = rate))
        
        update_dict[currency] = {
            "rate": float(rate)
        }
        
    except Exception as err:
        logging.warning("Get rate failed (GoogleFinance) SGD --> {currency}: {why}".format(currency = currency, why = err))

def get_rate_from_wise(currency: str, target: str, update_dict: dict):
    params = [('sourceCurrency', 'SGD'), ('targetCurrency', target), ("sendAmount", 100), ("providerType", "moneyTransferProvider")]
    resp = requests.get(wise_base_api, params=params)
    if not resp.ok:
        logging.error("API error (Wise) SGD --> {currency} with status code {code}: {msg}".format(currency = currency, code = resp.status_code, msg = resp.content))
        return
    
    jo = json.loads(resp.content)
    providers = jo.get('providers')
    if not providers:
        logging.warning("Empty providers (Wise) SGD --> {currency} with status code {code}: {msg}".format(currency = currency, code = resp.status_code, msg = resp.content))
        return
    
    for p in providers:
        if p.get('id') != wise_id:
            continue
        
        quote = p.get('quotes')[0]
        update_dict[currency] = {
            "fee": quote.get('fee'),
            "rate": quote.get('rate'),
            # "receivedAmount": quote.get('receivedAmount')
        }
        logging.info("(Wise) SGD --> {currency}: {rate} (rate), {fee} (fee)".format(currency = currency, rate = quote.get('rate'), fee = quote.get('fee')))
        return
        
    logging.warning("Wise not in providers (Wise) SGD --> {currency}: {msg}".format(currency = currency, msg = providers))
    
def update_influx(currency_dict: dict):
    pass
    # write_api = influx_client.write_api(write_options = SYNCHRONOUS)
    # point_list = []
    
    # for (platform, rate_dict) in currency_dict.items():
    #     for (currency, rate) in rate_dict.items():
    #         p = Point("currency").tag("source", "SGD").tag("target", currency).tag("platform", platform).field("rate", rate)
    #         point_list.append(p)
        
    # if not point_list:
    #     logging.error("Point list is empty, failed to update influx")
    #     return
            
    # try:
    #     write_api.write(bucket = bucket, org = org, record = point_list)
    
    # except Exception as err:
    #     logging.error("Write to Influx failed: {why}".format(why = err))

def update_firebase(currency_dict: dict):
    for (platform, rate_dict) in currency_dict.items():
        for (currency, rate_obj) in rate_dict.items():
            try:
                _ref = ref.child(currency)
                _ref.update({
                    # platform: {
                    #     "fee": rate.get("fee"),
                    #     "value": round(100*rate.get("value"), 3)
                    # }
                    platform: rate_obj
                })
            
            except ValueError as err:
                logging.error("ValueError in update Firebase: {why}".format(why = err))
    
            except TypeError as err:
                logging.error("TypeError in update Firebase: {why}".format(why = err))
            
            except FirebaseError as err:
                logging.error("FirebaseError in updating Firebase: {code} (code), {http} (http resp): {msg} ({cause})".format(code = err.code, http = err.http_response, msg = err, cause = err.cause))    

def init_logger():
    log_path = os.path.join(os.getcwd(), "log/crawler_v2.log")

    # Formatter for stdout logger
    stdout_formatter = logging.Formatter(f"[%(levelname)s] - %(message)s")

    # Formatter for file logger with timestamp
    file_formatter = logging.Formatter(f"%(asctime)s %(pathname)s (%(lineno)d %(funcName)s) [%(levelname)s] - %(message)s",
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

if __name__ == "__main__":
    init_logger()
    
    prev_time = time.time()
    while True:      
        currency_to_update = {}
        get_rate_thread_handlers = []
        for (platform, url_dict) in website_url.items():
            currency_to_update[platform] = {}
            for (currency, url) in url_dict.items():
                if platform == "GoogleFinance":
                    t = threading.Thread(target=get_rate_from_google_finance, args=(currency, url, currency_to_update[platform]), daemon=True)
                    t.start()
                    get_rate_thread_handlers.append(t)
                    
                elif platform == "Wise":
                    t = threading.Thread(target=get_rate_from_wise, args=(currency, url, currency_to_update[platform]), daemon=True)
                    t.start()
                    get_rate_thread_handlers.append(t)
        
        for t in get_rate_thread_handlers:
            t.join()
        
        # thread_influx = threading.Thread(target = update_influx, args = (currency_to_update,))
        # thread_influx.start()
        
        thread_firebase = threading.Thread(target = update_firebase, args = (currency_to_update,))
        thread_firebase.start()

        # thread_influx.join()
        thread_firebase.join()

        diff_in_time = time.time() - prev_time
        print(diff_in_time)
        
        if diff_in_time < 5:
            time.sleep(5 - diff_in_time)
            
        prev_time = time.time()