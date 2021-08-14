import pandas
import requests
import io
# import datetime as dt
import os

stockId = 'EZU'
apiKey = "MA9RQHUA2YHPJ79Q"
# dateToDay = dt.datetime.today().strftime('%Y%m%d')
print("start")
folderPath=os.getcwd() 

def dataframeFromUrl(url):
    dataString = requests.get(url).content
    parsedResult = pandas.read_csv(io.StringIO(dataString.decode('utf-8')), index_col=0)
    return parsedResult

def stockPriceIntraday(ticker, folder):
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval=1min&apikey={apiKey}&datatype=csv&outputsize=full'.format(ticker=ticker, apiKey=apiKey)
    intraday = dataframeFromUrl(url)
    print(url)
    file = folder+'/'+ticker+'.csv'
    print(file)
    if os.path.exists(file):
            history = pandas.read_csv(file, index_col=0)
            intraday.append(history)

    intraday.sort_index(inplace=True)

    intraday.to_csv(file)
    print('Intraday for [' + ticker + '] got.')

stockPriceIntraday(stockId, folder=folderPath)