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
        self.last_sent_rate = None
        self.percentage_increase = None

def query_last_sent_rate(source, target) -> float:
    try:
        mydb = get_connection_pool()
        sql_cursor = mydb.cursor()
    
    except Exception as err:
        logger.error("Connection to MySQL db failed: {err}".format(err = err))
        return None
    
    try:
        sql = "SELECT exchange_rate FROM history_rate WHERE from_currency=%s AND to_currency=%s ORDER BY id DESC LIMIT 1"
        val = (source, target)
        sql_cursor.execute(sql, val)
        row = sql_cursor.fetchone()
        
    except Exception as err:
        logger.error("Error fetching date from MySQL db: {err}".format(err = err))
        sql_cursor.close()
        mydb.close()
        return None
    
    if row is None:
        logger.info("%s --> %s last sent was None", source, target)
        return None
    
    sql_cursor.close()
    mydb.close()
    
    return row[0]

def query_average_rate_of_yesterday(source,target) -> float:
    query_api = influx_client.query_api()
    
    today_date = datetime.date.today()
    yesterday_date = today_date + datetime.timedelta(days=-1)
    
    start_time = datetime.datetime(
        year = yesterday_date.year,
        month = yesterday_date.month,
        day = yesterday_date.day
    ).astimezone(datetime.timezone.utc).isoformat()
    
    stop_time = datetime.datetime(
        year = today_date.year,
        month = today_date.month,
        day = today_date.day
    ).astimezone(datetime.timezone.utc).isoformat()
    
    query = 'from(bucket: "{bucket}")\
        |> range(start: {start_time}, stop: {stop_time})\
        |> filter(fn: (r) => r._measurement == "currency")\
        |> filter(fn: (r) => r.source == "{source}")\
        |> filter(fn: (r) => r.target == "{target}")\
        |> mean()'.format(
            bucket = bucket,
            start_time = start_time,
            stop_time = stop_time,
            source = source,
            target = target
        )
        
    try:
        table = query_api.query(org=org, query=query)
        result = table.to_values(columns=['_value'])
        return float(result[0][0])
    
    except Exception as err:
        logger.error("Query yesterday average error: {err}".format(err = err))
        return None
    
def query_rate_of_the_day(source, target) -> Rate:
    # yesterday_average = query_average_rate_of_yesterday(source, target)
    last_sent_rate = query_last_sent_rate(source, target)
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
        
        # if yesterday_average is not None:
        #     percentage_increase = ((now - yesterday_average) / yesterday_average) * 100
        #     rate.yesterday_average = yesterday_average
        #     rate.percentage_increase = percentage_increase
        if last_sent_rate is not None:
            percentage_increase = ((now - last_sent_rate) / last_sent_rate) * 100
            rate.last_sent_rate = last_sent_rate
            rate.percentage_increase = percentage_increase
        
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
    # yesterday_average = rate.yesterday_average
    last_sent_rate = rate.last_sent_rate
    percentage_increase = rate.percentage_increase
    
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
    # message = message + "[Max] {rate:.4f} at {time}\n".format(rate = max, time = max_time.strftime("%I:%M %p"))
    # message = message + "[Min] {rate:.4f} at {time}\n".format(rate = min, time = min_time.strftime("%I:%M %p"))
    # message = message + "[Average] {rate:.4f}\n".format(rate = mean)
    message = message + "<b>[Now] {rate:.4f}</b>".format(rate = current_rate)
    
    if percentage_increase is not None:
        # message = message + "\n[Ytd Avg] {rate:.4f}".format(rate = yesterday_average)
        message = message + "\n[Ytd] {rate:.4f}".format(rate = last_sent_rate)
        message = message + "\n\nIncreased by {percent:.2f}% (compared to yesterday)".format(percent = percentage_increase)
    
    for row in rows_list:
        try: 
            my_bot.send_message(row[0],
                                text = message,
                                parse_mode = "HTML")
        
        except Exception as err:
            logger.error("Send notification on Telegram failed: {err}".format(err = err))
            logger.error("chat_id: {id}".format(id = row[0]))

def update_last_sent(target_currency, rate: Rate):
    current_rate = rate.now
    try:
        mydb = get_connection_pool()
        sql_cursor = mydb.cursor()
        
    except Exception as err:
        logger.error("Connection to MySQL db failed: {err}".format(err = err))
        return
    
    sql = "INSERT INTO history_rate (from_currency, to_currency, exchange_rate) VALUES (%s, %s, %s)"
    val = ('SGD', target_currency, current_rate)
    sql_cursor.execute(sql, val)
    mydb.commit()
    
    sql_cursor.close()
    mydb.close()

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
        update_last_sent(t, r)

    logger.info("Published successfully")