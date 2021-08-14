import requests
import traceback
import datetime as dt
import pytz
import os
import time
import yfinance as yf
# importing pandas as pdls

import time as _time
import pandas as pd  # need also install yfinance lxml



def log_date(message):
    print(dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " : " + str(message))


def log_error(e):
    log_date("exception trace:")
    traceback.print_exc()

start_time = str(dt.datetime.now().strftime("%d%m%Y%H%M%S"))
# check unix timestamp https://www.epochconverter.com/\
# pip3 install -r requirements.txt

# stockId = '^N225'
# datetime_string='200702 04:01'

sleep_time=0.001 # in sec
stock_list_file_path = "./stock_list_for_day_history_light.xlsx"
stock_list_df = pd.read_excel(stock_list_file_path)
# print(stock_list_df)

# datetime_format = '%y%m%d %H:%M'
local_tz = pytz.timezone("Asia/Hong_Kong")

max_row_size=0
all_stock_dist = {}
for row_index, row_data_array in stock_list_df.iterrows():
    stockId = str(row_data_array[0])
    date_start_string = str(row_data_array[1])
    date_end_string = str(row_data_array[2])
    # log_date("row_index " + str(row_index))
    # log_date("row_data_array " + stockId)
    # log_date("row_data_array " + date_start_string)
    # log_date("row_data_array " + date_end_string)
    try:
        time.sleep(sleep_time) 
        msft = yf.Ticker(stockId)
        log_date(f"start checking stock: {stockId} , start: {date_start_string}, end: {date_end_string}")
        result_df = msft.history(interval="1d", start=date_start_string, end=date_end_string, auto_adjust=False)
        # print(result_df),
        # print(result_df.index)
        # print(result_df.index[0])
        # print(result_df.columns)
        # print(result_df.columns[0:4])
        # print(result_df.values)
        # print(result_df.iloc[0])
        # print(result_df.iloc[[0]])
        print(len(result_df))
        # print(result_df.loc[:, 'Open': 'Volume'])

        log_date("fetching result")
        log_date(result_df)
        # slice some column as we only need till Volume
        history_data_row_array = result_df.loc[:, 'Open': 'Volume'].values.tolist()
        # print(history_data_row_array)

        this_stock_data_result = []
        history_data_row_array_len = len(history_data_row_array)
        # for building correct row size dataFrame
        if history_data_row_array_len > max_row_size:
            max_row_size = history_data_row_array_len

        for i in range(history_data_row_array_len):
            row = history_data_row_array[i]
            # print(row)
            data_date = str(dt.datetime.fromtimestamp(result_df.index[i].timestamp(), pytz.timezone("Asia/Hong_Kong")))
            full_row = [stockId, data_date] + row
            full_row.append('') # seperator empty column
            # print(full_row)
            this_stock_data_result.append(full_row)

        all_stock_dist[stockId]=this_stock_data_result
        # print(all_stock_dist)

        # print(all_stock_dist)      

        # fill empty data for unbalance row
        for key in all_stock_dist:
            stock_row_len = len(all_stock_dist[key])
            if stock_row_len < max_row_size:
                row_to_pack = max_row_size-stock_row_len
                for w in range(row_to_pack):
                    all_stock_dist[key].append(['','','','','','','','','']) # len of empty column to pack is 8 + 1 empty seperator
    except Exception as e:
        log_error(e)
# print(all_stock_dist) 
## build find df list
# build empty data list first
excel_df_data_list=[]
excel_df_header_list=[]
header_result=[]  
header_result.append("stock_id")
header_result.append("date")
header_result.append("open")
header_result.append("high")
header_result.append("low")
header_result.append("close")
header_result.append("adj Close")
header_result.append("volume")
header_result.append("")
for i in range(max_row_size):
    excel_df_data_list.append([])

for key in all_stock_dist:
    stock_row_len = len(all_stock_dist[key])
    for w in range(stock_row_len):
        excel_df_data_list[w] = excel_df_data_list[w] + all_stock_dist[key][w]
    excel_df_header_list= excel_df_header_list + header_result

log_date("all done!!!")
# print(excel_df_data_list)
# print(excel_df_header_list)
df_one_row = pd.DataFrame(excel_df_data_list, columns=excel_df_header_list)
# print(df_one_row)
# one row format
# print(df_one_row.to_csv(index=False))
# output_file_name = f"output_{start_time}.xlsx"
output_file_name = f"stock_list_for_day_history_output.xlsx"
df_one_row.to_excel(output_file_name, index=False, engine="openpyxl")
full_output_path = os.getcwd() + os.path.sep + output_file_name
log_date(f'output file to path: {full_output_path}')

