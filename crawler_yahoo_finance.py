#!/usr/bin/python
import sys
import urllib2
import re
import os
import time
import random
import json
from yahoo_historical import Fetcher


# output file name: input/stockPrices_raw.json
# json structure: crawl daily price data from yahoo finance
#          ticker
#         /  |   \       
#     open close adjust ...
#       /    |     \
#    dates dates  dates ...

def calc_finished_ticker():
    os.system("awk -F',' '{print $1}' ./input/news_reuters.csv | sort | uniq > ./input/finished.reuters")


def get_stock_Prices():
    fin = open('./input/finished.reuters')
    output = './input/stockPrices_raw.json'

    # exit if the output already existed
    if os.path.isfile(output):
        sys.exit("Prices data already existed!")

    priceSet = {}
    priceSet['^GSPC'] = repeatDownload('^GSPC')  # download S&P 500
    for num, line in enumerate(fin):
        ticker = line.strip()
        priceSet[ticker] = repeatDownload(ticker)
        print(num, ticker, priceSet[ticker].shape)
        # if num > 10: break # for testing purpose

    with open(output, 'w') as outfile:
        json.dump(priceSet, outfile, indent=4)


def repeatDownload(ticker):
    repeat_times = 3  # repeat download for N times
    priceStr = []
    for _ in range(repeat_times):
        try:
            time.sleep(random.uniform(2, 3))
            priceStr = PRICE(ticker)
            if len(priceStr) > 0:  # skip loop if data is not empty
                break
        except Exception as e:
            if _ == 0: print ticker, "Http error! " + str(e)
    return priceStr


def PRICE(ticker):
    # start_y, start_m, start_d = '2004', '01', '01'  # starting date
    # end_y, end_m, end_d = '2999', '12', '01'  # until now
    #
    # # Construct url
    # # url1 = "http://chart.finance.yahoo.com/table.csv?s=" + ticker
    # # url2 = "&a=" + start_m + "&b=" + start_d + "&c=" + start_y
    # # url3 = "&d=" + end_m + "&e=" + end_d + "&f=" + end_y + "&g=d&ignore=.csv"
    # url = 'https://query1.finance.yahoo.com/v7/finance/download/{}?period1=1072886400&period2=1511107200&interval=1d&events=history&crumb=ZGHNKXN1VL6'
    # url = url.format(ticker)
    # # parse url
    # response = urllib2.urlopen(url)
    # csv = response.read().split('\n')
    # # get historical price
    # ticker_price = {}
    # index = ['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']
    # for num, line in enumerate(csv):
    #     line = line.strip().split(',')
    #     if len(line) < 7 or num == 0: continue
    #     date = line[0]
    #     # check if the date type matched with the standard type
    #     if not re.search(r'^[12]\d{3}-[01]\d-[0123]\d$', date): continue
    #     # open, high, low, close, volume, adjClose : 1,2,3,4,5,6
    #     for num, typeName in enumerate(index):
    #         try:
    #             ticker_price[typeName][date] = round(float(line[num + 1]), 2)
    #         except:
    #             ticker_price[typeName] = {}
    # return ticker_price
    data = Fetcher(ticker, [2004, 1, 1], [2999, 12, 31])
    return data.getHistorical()


if __name__ == "__main__":
    # calc_finished_ticker()
    get_stock_Prices()
