#import dependencies
from pandas_datareader import data
import matplotlib.pyplot as plt
import pandas as pd
import string
import datetime as dt
import requests
import json
import os
import numpy as np
from bs4 import BeautifulSoup
# import tensorflow as tf
from sklearn.preprocessing import MinMaxScaler

api_key = "QUCHJO09418JZSB4"

#Decide if want all stocks or just one
all_symbols = 0

#Get a current list of stock symbols for NASDAQ
alpha = list(string.ascii_uppercase)

symbols = []
symbols_clean = []

def get_all_stocks(alpha):
    for each in alpha:
        url = 'http://eoddata.com/stocklist/NASDAQ/{}.htm'.format(each)
        resp = requests.get(url)
        site = resp.content
        soup = BeautifulSoup(site, 'html.parser')
        table = soup.find('table', {'class': 'quotes'})
        for row in table.findAll('tr')[1:]:
            symbols.append(row.findAll('td')[0].text.rstrip())
    
    for each in symbols:
        each = each.replace('.','-')
        symbols_clean.append((each.split('-')[0]))
    
    return symbols_clean

#Clean extra letters on the end of the symbol

symbols_test = ["AAL"]



def get_prices(symbol):
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'outputsize': 'full',
        'datatype': 'json',
        'apikey': api_key
    }
    url = "https://www.alphavantage.co/query"
    try:
        response = requests.get(url, params=params)
        print(f"Processing {url}...")
        response.raise_for_status()
        print(f"Response status for request on {symbol}: {response.status_code}")
    except Exception as err:
        print(f"An error has occurred: {err}")
    response_json = response.json()
    return response_json

def extract_json(data):
    symbol = data['Meta Data']['2. Symbol']
    file_to_save = '%s-pricehistory.csv'%symbol
    data = data['Time Series (Daily)']
    df = pd.DataFrame(columns=['Date', 'Low', 'High', 'Close', 'Open'])
    for k, v in data.items():
        date = dt.datetime.strptime(k, '%Y-%m-%d')
        data_row = [date.date(), float(v['3. low']), float(v['2. high']), float(v['4. close']), float(v['1. open'])]
        df.loc[-1,:] = data_row
        df.index = df.index + 1
    my_path = os.path.abspath(os.path.dirname(__file__))
    path = os.path.join(my_path, "../data_alphavantage")
    df.to_csv(r"{}/{}".format(path,file_to_save), index=False)
    return 'Data is saved to data_alphavantage/%s'%file_to_save

def main():
    if(all_symbols == 1):
        symbols_cleaned = get_all_stocks(symbols_clean)
        for symbol in symbols_cleaned:
            tasks = get_prices(symbol)
            extract_json(tasks)
    elif(all_symbols == 0):
        tasks = get_prices(symbols_test)
        extract_json(tasks)

if __name__ == '__main__':
    main()
