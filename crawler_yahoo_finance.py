#!/usr/bin/python
import sys
import urllib2
import re
import os
import time
import random
import json
from yahoo_historical import Fetcher
import cPickle as pickle
from collections import OrderedDict


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
    # output = './input/stockPrices_raw.json'
    output = './input/stockPrices_raw.pkl'

    # exit if the output already existed
    if os.path.isfile(output):
        sys.exit("Prices data already existed!")

    priceSet = OrderedDict()
    priceSet['^GSPC'] = repeatDownload('^GSPC')  # download S&P 500
    for num, line in enumerate(fin):
        ticker = line.strip()
        priceSet[ticker] = repeatDownload(ticker)
        print(num, ticker, len(priceSet[ticker]))
        # if num > 10: break # for testing purpose

    pickle.dump(priceSet, open(output, 'w'))


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
    fetcher = Fetcher(ticker, [2004, 1, 1], [2999, 12, 31])
    data = fetcher.getHistorical()
    data.set_index('Date', inplace=True)
    return data


if __name__ == "__main__":
    # calc_finished_ticker()
    get_stock_Prices()
