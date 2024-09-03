import os
import sys
import datetime
import numpy as np
import logging
from logging.handlers import RotatingFileHandler
from database.influx import client as influx_client
from database.influx import bucket, org
from database.mysql import get_connection_pool
from tukar_wang_bot import my_bot
from common.util import flags, available_currency_dict

class Rate:
    def __init__(self, 
                 now: float,
                 mean: float, 
                 max: float,
                 max_time: datetime.datetime, 
                 min: float, 
                 min_time: datetime.datetime):
        self.now = now
        self.mean = mean
        self.max = max
        self.min = min
        self.max_time = max_time
        self.min_time = min_time
    
def query_rate_of_the_day(source, target) -> Rate:
    query_api = influx_client.query_api()
    
    today_date = datetime.date.today()
    start_time = datetime.datetime(
        year = today_date.year,
        month = today_date.month,
        day = today_date.day
    ).astimezone(datetime.timezone.utc).isoformat()
    
    query = 'from(bucket:"{bucket}")\
        |> range(start: {start_time})\
        |> filter(fn: (r) => r._measurement == "currency")\
        |> filter(fn: (r) => r.source == "{source}")\
        |> filter(fn: (r) => r.target == "{target}")\
        |> filter(fn: (r) => r._field == "rate")'.format(
            bucket = bucket,
            start_time = start_time,
            source = source,
            target = target
        )
    
    try:
        table = query_api.query(org=org, query=query)
        result = np.array(table.to_values(columns=['_time', '_value']))
        
        max_idx = result[:, 1:].argmax()
        min_idx = result[:, 1:].argmin()
        
        tz_delta = datetime.timedelta(hours = 8)
        max_time: datetime.datetime = result[max_idx][0] + tz_delta
        min_time: datetime.datetime = result[min_idx][0] + tz_delta
        
        max = result[max_idx][1]
        min = result[min_idx][1]
        mean = float(result[:, 1:].mean())
        now = float(result[-1][1])
        
        rate = Rate(now = now,
                    mean = mean,
                    max = max,
                    max_time = max_time,
                    min = min,
                    min_time = min_time)
        
        return rate
    
    except Exception as err:
        logger.error("Query influx error: {err}".format(err = err))
        sys.exit(1)

def broadcast(target_currency, rate: Rate):
    current_rate = rate.now
    mean = rate.mean
    max = rate.max
    max_time = rate.max_time
    min = rate.min
    min_time = rate.min_time
    
    now = datetime.datetime.now()
    
    try:
        mydb = get_connection_pool()
        sql_cursor = mydb.cursor()
    
    except Exception as err:
        logger.error("Connection to MySQL db failed: {err}".format(err = err))
        sys.exit(1)
    
    try:
        sql = "SELECT chat_id FROM userinfo WHERE to_currency=%s AND is_active=1"
        val = (target_currency,)
        sql_cursor.execute(sql, val)
        rows_list = sql_cursor.fetchall()
    
    except Exception as err:
        logger.error("Fetch result from MySQL db failed: {err}".format(err = err))
        sys.exit(1)
    
    message = "<u>{src_flag}SGD --> {target_flag}{currency} ({time})</u>\n".format(src_flag = flags["SGD"], target_flag = flags[target_currency], currency = target_currency, time = now.strftime("%Y-%m-%d"))
    message = message + "[Max] {rate:.4f} at {time}\n".format(rate = max, time = max_time.strftime("%I:%M %p"))
    message = message + "[Min] {rate:.4f} at {time}\n".format(rate = min, time = min_time.strftime("%I:%M %p"))
    message = message + "[Average] {rate:.4f}\n".format(rate = mean)
    message = message + "[Now] {rate:.4f}".format(rate = current_rate)
    
    try:
        for row in rows_list:
            my_bot.send_message(row[0],
                                text = message,
                                parse_mode = "HTML")
    
    except Exception as err:
        logger.error("Send notification on Telegram failed: {err}".format(err = err))
        sys.exit(1)

if __name__ == "__main__":
    log_path = os.path.join(os.getcwd(), "log/notifier.log")
    
    # Formatter for file logger with timestamp
    file_formatter = logging.Formatter(f"%(asctime)s %(pathname)s [%(levelname)s] - %(message)s",
                                    datefmt="%Y-%m-%d %H:%M:%S")

    # Create rotating file handler with DEBUG level, max size 10MB, and backup count of 10 files
    file_handler = RotatingFileHandler(log_path, maxBytes=10 * 1024 * 1024, backupCount=10)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_formatter)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    
    rate = []
    for target in available_currency_dict.values():
        rate.append((target, query_rate_of_the_day('SGD', target)))
    
    # Separate so that the subsequent message doesn't take too long to send
    for (t, r) in rate:
        broadcast(t, r)

    logger.info("Published successfully")