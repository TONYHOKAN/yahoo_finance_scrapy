import requests
import traceback
import datetime as dt
import pytz
import os
import time
# importing pandas as pd

import pandas as pd  # need also install xlrd openpyxl



def log_date(message):
    print(dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " : " + str(message))


def log_error(e):
    log_date("exception trace:")
    traceback.print_exc()

def append_header(header_result_array):
    header_result_array.append("stock_id")
    header_result_array.append("date")
    header_result_array.append("open")
    header_result_array.append("high")
    header_result_array.append("low")
    header_result_array.append("close")
    header_result_array.append("volume")
    header_result_array.append("")


start_time = str(dt.datetime.now().strftime("%d%m%Y%H%M%S"))
# check unix timestamp https://www.epochconverter.com/\
# pip3 install -r requirements.txt

# stockId = '^N225'
# datetime_string='200702 04:01'

sleep_time=0.001 # in sec
stock_list_file_path = "./stock_list.xlsx"
stock_list_df = pd.read_excel(stock_list_file_path)
# print(stock_list_df)

datetime_format = '%y%m%d %H:%M'
local_tz = pytz.timezone("Asia/Hong_Kong")

# define result dist
stock_id = []
date = []
low = []
open = []
close = []
volume = []
high = []
dict = {
    'stock_id': stock_id,
    'date': date,
    'open': open,
    'high': high,
    'low': low,
    'close': close,
    'volume': volume
}

header_result=[]
find_result=[]

for row_index, row_data_array in stock_list_df.iterrows():
    stockId = str(row_data_array[0])
    datetime_string = str(row_data_array[1])
    log_date("row_index " + str(row_index))
    log_date("row_data_array " + stockId)
    log_date("row_data_array " + datetime_string)

    # snapshot datetime to HK UTC+8
    datetime_without_tz = dt.datetime.strptime(datetime_string, datetime_format)
    datetime_with_tz = local_tz.localize(
        datetime_without_tz, is_dst=None)  # No daylight saving time
    date_in_timestamp = datetime_with_tz.timestamp()
    log_date(
        f"checking in HK timezonedate: {datetime_with_tz}, timestamp: {date_in_timestamp}, for ticket: {stockId}")

    date_start_in_linux_timestamp = int(date_in_timestamp)
    # add 60 sec to next minutes
    date_end_in_linux_timestamp = int(date_start_in_linux_timestamp + 60)
    # log_date(f"date_start_in_linux_timestamp: {date_start_in_linux_timestamp}")
    # fromtimestamp always convert to local timezone, aka HK UTC + 8
    # log_date(dt.datetime.fromtimestamp(date_start_in_linux_timestamp))
    # log_date(f"date_end_in_linux_timestamp: {date_end_in_linux_timestamp}")
    # log_date(dt.datetime.fromtimestamp(date_end_in_linux_timestamp))

    # below yahoo API at least will return range from start mins + 1 days
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{stockId}?symbol={stockId}&period1={date_start_in_linux_timestamp}&period2={date_end_in_linux_timestamp}&interval=1m&includePrePost=true&events=div%7Csplit%7Cearn&lang=en-US&region=HK&crumb=E9KQpesHoq%2F&corsDomain=finance.yahoo.com"
    # log_date(url)
    stock_find_or_not = False
    try:
        time.sleep(sleep_time) # avoid heavily call
        response_json = requests.get(url).json()
        # log_date(response_json)
        if response_json["chart"]["result"] is None:
            log_date(f"No ticket for {stockId}. " + str(response_json))
        elif response_json["chart"]["result"][0].get("timestamp") is None:
            log_date(
                f"No result for Ticker: {stockId}, in HK timezonedate: {datetime_with_tz}, timestamp: {date_in_timestamp}")
        else:
            timestamp = response_json["chart"]["result"][0]["timestamp"]
            # log_date(timestamp)
            check_timestamp_position = 0

            log_date(f"start check result for ticket: {stockId}")
            for i in range(len(timestamp)):
                t = timestamp[i]
                # log_date(t)
                # log_date(f"date: {dt.datetime.fromtimestamp(t)}, timestamp: {t}") # this by default convert to HK timezone
                if t == date_in_timestamp:
                    log_date("find!!")
                    log_date(
                        f"checking Ticker: {stockId}, in HK timezonedate: {datetime_with_tz}, timestamp: {date_in_timestamp} and result:")
                    indicator = response_json["chart"]["result"][0]["indicators"]["quote"][0]

                    
                    stock_date = str(dt.datetime.fromtimestamp(
                        date_in_timestamp, pytz.timezone("Asia/Hong_Kong")))
                    
                    stock_low=indicator["low"][i]
                    stock_open=indicator["open"][i]
                    stock_close=indicator["close"][i]
                    stock_volume=indicator["volume"][i]
                    stock_high=indicator["high"][i]
                    # stock_id.append(stockId)
                    # date.append(stock_date)
                    # low.append(stock_low)
                    # open.append(stock_open)
                    # close.append(stock_close)
                    # volume.append(stock_volume)
                    # high.append(stock_high)

                    log_date(f"open: {stock_open}")
                    log_date(f"high: {stock_high}")
                    log_date(f"low: {stock_low}")
                    log_date(f"close: {stock_close}")
                    log_date(f"volume: {stock_volume}")

                    append_header(header_result)

                    find_result.append(stockId)
                    find_result.append(stock_date)
                    find_result.append(stock_open)
                    find_result.append(stock_high)
                    find_result.append(stock_low)
                    find_result.append(stock_close)
                    find_result.append(stock_volume)
                    find_result.append("")
                    stock_find_or_not = True
                break
    except Exception as e:
        log_error(e)
    if stock_find_or_not is False:
        log_date("fill empty header")
        append_header(header_result)
        find_result.append(stockId)
        find_result.append(stock_date)
        find_result.append("")
        find_result.append("")
        find_result.append("")
        find_result.append("")
        find_result.append("")
        find_result.append("")
        
df = pd.DataFrame(dict)
log_date('')
log_date('---CSV format---')
print()
print(df.to_csv(index=False))

# csv format
# output_file_name = f"output_{start_time}.xlsx"
# df.to_excel(output_file_name, index=False, engine="openpyxl")
# full_output_path = os.getcwd() + os.path.sep + output_file_name
# log_date(f'output file to path: {full_output_path}')

# one row format
df_one_row = pd.DataFrame([find_result], columns=header_result)
# print(df_one_row.to_csv(index=False))
# output_file_name = f"output_{start_time}.xlsx"
output_file_name = f"stock_list_output.xlsx"
df_one_row.to_excel(output_file_name, index=False, engine="openpyxl")
full_output_path = os.getcwd() + os.path.sep + output_file_name
log_date(f'output file to path: {full_output_path}')
