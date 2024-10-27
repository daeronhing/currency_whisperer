import os
import sys
import time
import logging
import telebot
import datetime
import numpy as np
from logging.handlers import RotatingFileHandler
from common.util import flags, available_currency_dict
from database.influx import client as influx_client
from database.influx import bucket, org
from database.mysql import get_connection_pool
from telebot.types import ReplyKeyboardMarkup, KeyboardButton

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
my_bot = telebot.TeleBot(BOT_TOKEN)

@my_bot.message_handler(commands=['help'])
def help(msg: telebot.types.Message):
    reply = "Hi {name}👋, nice to meet you!😁\n".format(name=msg.chat.username)
    reply = reply + "\nI am designed to keep track of the latest currency exchange rates for you!\n"
    reply = reply + "\nAfter you subscribe, I will send you the daily currency notifications at <b>*6PM*</b>.\n"
    reply = reply + "\n<u>Available Commands:</u>\n"
    reply = reply + "/start - Start subscription\n"
    # reply = reply + "/jpy - Subscribe to SGD --> JPY\n"
    # reply = reply + "/myr - Subscribe to SGD --> MYR\n"
    reply = reply + "/now - Receive the current exchange rate you are subscribing to\n"
    reply = reply + "/convert - Calculate how much your SGD convert into\n"
    reply = reply + "/unsubscribe - Unsubscribe\n"
    
    my_bot.send_message(msg.chat.id,
                     text = reply,
                     parse_mode = "HTML")

@my_bot.message_handler(commands=['now'])
def now(msg: telebot.types.Message):
    username = msg.chat.username
    chat_id = msg.chat.id
    
    my_bot.send_chat_action(
        chat_id = chat_id,
        action = "typing"
    )
    
    try:
        mydb = get_connection_pool()
        sql_cursor = mydb.cursor()
        
    except Exception as err:
        logger.error("Connection to MySQL db failed: {err}".format(err = err))
        my_bot.send_message(chat_id,
                            text="Please try again later🥴")
        sql_cursor.close()
        mydb.close()
        return
        
    sql = "SELECT to_currency FROM userinfo WHERE is_active=1 AND chat_id=%s"
    val = (chat_id,)
    try:
        sql_cursor.execute(sql, val)
        rows_list = sql_cursor.fetchall()
    
    except Exception as err:
        logger.error("Fetching data from MySQL db failed: {err}".format(err = err))
        my_bot.send_message(chat_id,
                            text="Please try again later🥴")
        sql_cursor.close()
        mydb.close()
        return
        
    try:
        query_api = influx_client.query_api()
        for row in rows_list:
            target_currency = row[0]
            query = 'from(bucket:"{bucket}")\
                    |> range(start: -15s)\
                    |> filter(fn: (r) => r._measurement == "currency")\
                    |> filter(fn: (r) => r.source == "SGD")\
                    |> filter(fn: (r) => r.target == "{target}")\
                    |> filter(fn: (r) => r._field == "rate")\
                    |> last()'.format(
                        bucket = bucket,
                        target = target_currency
                    )
            table = query_api.query(org = org, query = query)
            result = np.array(table.to_values(columns=['_time', '_value']))
            rate = result[-1][1]
            
            logger.info("{username} (id: {chat_id}) replied with rate {rate:.3f}".format(username = username,
                                                                                    chat_id = chat_id, 
                                                                                    rate = rate))
            reply = "{src_flag}SGD --> {target_flag}{currency}: {rate:.3f}".format(src_flag=flags["SGD"], target_flag=flags[target_currency],currency = target_currency, 
                                                                    rate=rate)
            my_bot.send_message(chat_id,
                            text=reply)
            
    except Exception as err:
        logger.error("/now : {err}".format(err = err))
        my_bot.send_message(chat_id,
                            text="Please try again later🥴")
        
    sql_cursor.close()
    mydb.close()

# @my_bot.message_handler(commands=['jpy', 'myr', 'cny', 'twd', 'gbp'])
@my_bot.message_handler(commands=list(map(lambda s: s.lower(), available_currency_dict.values())))
def subscribe_to(msg: telebot.types.Message):
    username = msg.chat.username
    chat_id = msg.chat.id
    currency = msg.text.strip('/')
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    if currency in available_currency_dict.keys():
        currency = available_currency_dict.get(currency)
        
    elif currency.upper() in available_currency_dict.values():
        currency = currency.upper()
        
    else:
        my_bot.send_message(chat_id,
                            text="This currency is not available yet")
        return
    
    my_bot.send_chat_action(
        chat_id = chat_id,
        action = "typing"
    )
    
    try:
        mydb = get_connection_pool()
        sql_cursor = mydb.cursor()
    
    except Exception as err:
        logger.error("Connection to MySQL db failed: {err}".format(err = err))
        my_bot.send_message(chat_id,
                            text="Please try again later🥴")
        sql_cursor.close()
        mydb.close()
        return
        
    sql = "SELECT is_active FROM userinfo WHERE chat_id=%s AND to_currency=%s"
    val = (chat_id, currency)
    try:    
        sql_cursor.execute(sql, val)
        row = sql_cursor.fetchone()
    
    except Exception as err:
        logger.error("Error fetching data from MySQL db: {err}".format(err = err))
        my_bot.send_message(chat_id,
                            text="Please try again later🥴")
        sql_cursor.close()
        mydb.close()
        return
    
    if row is None:
        logger.info("%s (id: %s) subscribed to %s", username, chat_id, currency)
        sql = "INSERT INTO userinfo (username, chat_id, from_currency, to_currency, is_active, alert_time, activated_datetime) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (username, chat_id, 'SGD', currency, True, "18:00:00", now)
        sql_cursor.execute(sql, val)
        reply = "Thank you for subscribing🥳"
        mydb.commit()
        
    elif row[0]:
        logger.info("%s (id: %s) double subscribed %s", username, chat_id, currency)
        reply = "You have an active subscription🤭"
        
    else:
        logger.info("%s (id: %s) resume subscription for %s", username, chat_id, currency)
        sql = "UPDATE userinfo SET is_active=1, activated_datetime=%s WHERE chat_id=%s AND to_currency=%s"
        val = (now, chat_id, currency)
        sql_cursor.execute(sql, val)
        reply = "Subscription resumed🤗"
        mydb.commit()

    try:           
        my_bot.send_message(msg.chat.id,
                        text=reply)
                
    except Exception as err:
        logger.error("/jpy/myr:{err}".format(err = err))
        my_bot.send_message(chat_id,
                            text="Please try again later🥴")
        
    sql_cursor.close()
    mydb.close()

@my_bot.message_handler(commands=['unsubscribe'])
def unsubscribe_to(msg: telebot.types.Message):
    chat_id = msg.chat.id
    
    my_bot.send_chat_action(
        chat_id = chat_id,
        action = "typing"
    )
    
    try:
        mydb = get_connection_pool()
        sql_cursor = mydb.cursor()
        
    except Exception as err:
        logger.error("Connection to MySQL db failed: {err}".format(err = err))
        my_bot.send_message(chat_id,
                            text="Please try again later🥴")
    
    sql = "SELECT to_currency FROM userinfo WHERE chat_id=%s AND is_active=1"
    val = (chat_id, )
    
    try:
        sql_cursor.execute(sql, val)
        row_list = sql_cursor.fetchall()
    
    except Exception as err:
        logger.error("Fetch result from MySQL db failed: {err}".format(err = err))
    
    markup = ReplyKeyboardMarkup(row_width = 2, resize_keyboard = True, one_time_keyboard = True)
    currency_short_form = available_currency_dict.values()
    descriptive_form = available_currency_dict.keys()
    temp_dict = dict(zip(currency_short_form, descriptive_form))
    
    for row in row_list:
        button = temp_dict.get(row[0])
        markup.add(KeyboardButton(button))
    
    my_bot.send_message(chat_id,
                        "Which currency do you wish to unsubscribe?",
                        reply_markup = markup)
    
    sql_cursor.close()
    mydb.close()
    
    my_bot.register_next_step_handler(msg, last_check)
    # my_bot.send_message(msg.chat.id,
    #                     text="🚫🤪NO BACKSIES!!!")
    
def last_check(message: telebot.types.Message):
    username = message.chat.username
    chat_id = message.chat.id
    currency_long_form = message.text
    currency_short_form = available_currency_dict.get(currency_long_form)
    
    logger.info("%s (id: %s) unsubscribing %s", username, chat_id, currency_long_form)
    
    if currency_short_form is None:
        reply_text = "Have you previously subscribed to {currency}?🧐".format(currency = currency_long_form)
        my_bot.send_message(chat_id,
                            reply_text)
        return
    
    markup = ReplyKeyboardMarkup(row_width = 2, resize_keyboard = True, one_time_keyboard = True)
    markup.add(KeyboardButton("Yes"))
    markup.add(KeyboardButton("No"))
    
    confirmation_text = "Are you sure you want to unsubscribe {currency}?🤧".format(currency = currency_long_form)
    
    my_bot.send_message(chat_id,
                        confirmation_text,
                        reply_markup = markup)
    
    my_bot.register_next_step_handler(message, set_to_inactive, currency_short_form)
    
def set_to_inactive(message: telebot.types.Message, currency):
    username = message.chat.username
    chat_id = message.chat.id
    
    logger.info("%s (id: %s) chosen %s to unsubsribe %s", username, chat_id, message.text, currency)
    
    if message.text == "No":
        my_bot.send_message(chat_id,
                            "Phew, that was a close one😏")
        
    elif message.text == "Yes":
        my_bot.send_message(chat_id,
                            "Unsubscribing...")
        
        try:
            mydb = get_connection_pool()
            sql_cursor = mydb.cursor()
            
        except Exception as err:
            logger.error("Connection to MySQL db failed: {err}".format(err = err))
            my_bot.send_message(chat_id,
                                "Unsubscription failed. Please try again later🥴.")
        
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "UPDATE userinfo SET is_active=0, deactivated_datetime=%s WHERE chat_id=%s AND to_currency=%s"
        val = (now, chat_id, currency)
        
        try: 
            sql_cursor.execute(sql, val)
            mydb.commit()
        
        except Exception as err:
            logger.error("Failed to execute SQL for unsubscription, {err}".format(err = err))
            
        my_bot.send_message(chat_id,
                            "Unsubscribed🤧")
        
        sql_cursor.close()
        mydb.close()
        
    else:
        my_bot.send_message(chat_id,
                            "I don't understand🤯")

@my_bot.message_handler(commands=['test-emoji'])
def test_emoji(message):
    my_bot.register_next_step_handler(message, get_emoji_id)
    
def get_emoji_id(message: telebot.types.Message):
    my_bot.reply_to(message, message.text)
    logger.info(message.text)

def gen_target_currency_keyboard():
    markup = ReplyKeyboardMarkup(row_width = 2, resize_keyboard = True, one_time_keyboard = True)
    for currency in available_currency_dict.keys():
        markup.add(KeyboardButton(currency))
    return markup

@my_bot.message_handler(commands=["start"])
def start_command(message: telebot.types.Message):
    my_bot.send_message(
        message.chat.id, 
        "🧐What currency do you want to subscribe to?", 
        reply_markup = gen_target_currency_keyboard()
    )
    my_bot.register_next_step_handler(message, subscribe_to)
    
@my_bot.message_handler(commands=["convert"])
def convert(message: telebot.types.Message):
    my_bot.send_message(
        message.chat.id, 
        "What currency are you converting into?🧮", 
        reply_markup = gen_target_currency_keyboard()
    )
    my_bot.register_next_step_handler(message, get_user_conversion_target)

def get_user_conversion_target(message: telebot.types.Message):
    username = message.chat.username
    chat_id = message.chat.id
    currency = message.text.strip('/')
    
    if currency in available_currency_dict.keys():
        currency = available_currency_dict.get(currency)
    
    elif currency.upper() in available_currency_dict.values():
        currency = currency.upper()
        
    else:
        logger.warning("%s (id: %s) tried to convert weird currency %s", username, chat_id, currency)
        my_bot.send_message(chat_id,
                            text="This currency is not available yet")
        return

    my_bot.send_message(chat_id,
                        text="Please input the amount of SGD you are exchanging")
    
    my_bot.register_next_step_handler(message, calc_conversion, currency)
    
def calc_conversion(message: telebot.types.Message, currency):
    username = message.chat.username
    chat_id = message.chat.id
    try:
        amount = float(message.text)
        
    except ValueError:
        logger.warning("%s (id: %s) input weird number %s", username, chat_id, message.text)
        my_bot.send_message(chat_id,
                        text="Please input number only. Example: 123.45")
        return
    
    except Exception as err:
        logger.warning("Some error in calculator: {err}".format(err = err))
        my_bot.send_message(chat_id,
                            text="Please try again later🥴")
        return
    
    my_bot.send_chat_action(
        chat_id = chat_id,
        action = "typing"
    )
    
    try:
        query_api = influx_client.query_api()
        query = 'from(bucket:"{bucket}")\
                |> range(start: -15s)\
                |> filter(fn: (r) => r._measurement == "currency")\
                |> filter(fn: (r) => r.source == "SGD")\
                |> filter(fn: (r) => r.target == "{target}")\
                |> filter(fn: (r) => r._field == "rate")\
                |> last()'.format(
                    bucket = bucket,
                    target = currency
                )
        table = query_api.query(org = org, query = query)        
        result = np.array(table.to_values(columns=['_time', '_value']))
        rate = result[-1][1]
                
    except Exception as err:
        logger.error("conversion error: {err}".format(err = err))
        my_bot.send_message(chat_id,
                            text="Please try again later🥴")
        
    converted_amount = amount * rate
    reply = "For {amt} SGD, you get {converted:.2f} {currency}".format(amt = amount, converted = converted_amount, currency = currency)
    logger.info("%s (id: %s) requested a calculator: %s", username, chat_id, reply)
    my_bot.send_message(chat_id,
                        text=reply)

if __name__ == "__main__":
    log_path = os.path.join(os.getcwd(), "log/tukar_wang_bot.log")
    
    # Formatter for stdout logger
    stdout_formatter = logging.Formatter(f"[%(levelname)s] - %(message)s")

    # Formatter for file logger with timestamp
    file_formatter = logging.Formatter(f"%(asctime)s %(pathname)s [%(levelname)s] - %(message)s",
                                    datefmt="%Y-%m-%d %H:%M:%S")

    # Create stdout handler with INFO level
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.setFormatter(stdout_formatter)

    # Create rotating file handler with DEBUG level, max size 10MB, and backup count of 10 files
    file_handler = RotatingFileHandler(log_path, maxBytes=10 * 1024 * 1024, backupCount=10)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(stdout_handler)
    logger.addHandler(file_handler)
    
    while True:
        try:
            logger.info("Start bot polling")
            my_bot.infinity_polling()
        
        except Exception as err:
            logger.error("This catches the error in main: {e}".format(e = err))
            time.sleep(1)
            continue