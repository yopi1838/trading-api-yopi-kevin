import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import string
import time
from datetime import datetime
import requests
from google.cloud import bigquery
import os
import asyncio
import aiohttp
import json
import random
from aiohttp import ClientSession
from urllib.error import HTTPError

# Price History API Documentation
# https://developer.tdameritrade.com/price-history/apis/get/marketdata/%7Bsymbol%7D/pricehistory

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=r"C:\\Users\\Yopi-CEC\Desktop\\programming\\trading-api-yopi\\lib\\kevin-yopi-trading-api-bbe2ba990cb2.json"

def unix_time_millisec(dt):
    epoch = datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds() * 1000.0)

def chunks(l,n):
    """
    Takes list and how long do you want them
    
    Return:
    Chunked list
    """
    k = max(1,n)
    return (l[i:i+k] for i in range(0, len(l), k))

# Only doing one day here as an example
<<<<<<< HEAD
start_date = datetime.strptime('2000-01-01', '%Y-%m-%d')
end_date = datetime.strptime('2020-12-31', '%Y-%m-%d')
=======
start_date = datetime.strptime('2010-11-19', '%Y-%m-%d')
end_date = datetime.strptime('2012-11-19', '%Y-%m-%d')
>>>>>>> 24e87e1 (test)

# Convert to unix for the API
start_date_ms = unix_time_millisec(start_date)
end_date_ms = unix_time_millisec(end_date)

# Get a current list of all the stock symbols for the NYSE
alpha = list(string.ascii_uppercase)

symbols = []

for each in alpha:
    url = 'http://eoddata.com/stocklist/NASDAQ/{}.htm'.format(each)
    resp = requests.get(url)
    site = resp.content
    soup = BeautifulSoup(site, 'html.parser')
    table = soup.find('table', {'class': 'quotes'})
    for row in table.findAll('tr')[1:]:
        symbols.append(row.findAll('td')[0].text.rstrip())

# Remove the extra letters on the end
symbols_clean = []
# print(symbols)
symbols_randomized = random.sample(symbols, 480)

for each in symbols_randomized:
    each = each.replace('.', '-')
    symbols_clean.append((each.split('-')[0]))

symbol_aapl = ["AAL"]

# Get the price history for each stock. This can take a while
consumer_key = 'QUCHJO09418JZSB4'
params = {
    'apikey': consumer_key,
    'periodType': 'month',
    'frequencyType': 'daily',
    'frequency': 1,
    'startDate': start_date_ms,
    'endDate': end_date_ms,
    'needExtendedHoursData': 'true'
    }

sem = asyncio.BoundedSemaphore(120)
symbl_l, open_l, close_l, volume_l, date_l, high_l, low_l = [], [], [], [], [], [], []

async def get_pricehistory(symbol, session):
    async with sem:
        url = r"https://api.tdameritrade.com/v1/marketdata/{}/pricehistory".format(symbol)
        try:
            response = await session.request(method='GET', url=url, params=params)
            print(f"Processing {url}...")
            await asyncio.sleep(60)
            response.raise_for_status()
            print(f"Response status ({url}: {response.status}")
        except HTTPError as http_err:
            print(f"HTTP error occcured: {http_err}")
        except Exception as err:
            print(f"An error has occurred: {err}")
        response_json = await response.json()
        return response_json

def extract_responses(response):
    """
    Extract API responses
    
    Input:
    a JSON response from TD Ameritrade API taken for each symbol listed in NYSE

    Return:
    symbl_l = list of symbols
    open_l = list of open prices corresponding to symbol in symbl_l
    close_l = list of close prices corresponding to symbol in symbl_l
    volume_l = list of volumes corresponding to symbol in symbl_l
    date_l = recorded date taken for each symbol

    """
    # symbol = "test"
    symbol = response.get("symbol")
    candle = response.get("candles", [{}])
    for i in range(len(candle)):
        open_price = response.get("candles", [{}])[i]['open']
        close_price = response.get("candles", [{}])[i]['close']
        high = response.get("candles", [{}])[i]['high']
        low = response.get("candles", [{}])[i]['low']
        volume = response.get("candles", [{}])[i]['volume']
        date = response.get("candles", [{}])[i]['datetime']

        symbl_l.append(symbol)
        open_l.append(open_price)
        close_l.append(close_price)
        high_l.append(high)
        low_l.append(low)
        volume_l.append(volume)
        date_l.append(date)

    df = pd.DataFrame({
        'symbol': symbl_l,
        'open': open_l,
        'close': close_l,
        'high': high_l,
        'low': low_l,
        'volume': volume_l,
        'date': date_l
    })
    df['date']=pd.to_datetime(df['date'], unit='ms')
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    df = df.dropna()

    df1 = df[df['symbol'] == symbol]
    my_path = os.path.abspath(os.path.dirname(__file__))
    path = os.path.join(my_path, "../data")
    df1.to_csv(r"{}/{}2_pricehistory.csv".format(path, symbol), index=False)
    return 'Parsed!'

async def run_program(symbol, session):
    """
    A wrapper function for running program in asynchronous manner
    
    Input:
    symbol: corresponding stock symbol from symbol_clean
    session: retrieval URL session

    Return: 
    None
    """
    try:
        response = await get_pricehistory(symbol, session)
        extract_responses(response)
    except Exception as err:
        print(f"Exception occurred: {err}")
        pass

async def run_session():
    async with ClientSession() as session:
        tasks = [run_program(symbol, session) for symbol in symbol_aapl]
        await asyncio.gather(*tasks)
        
def main():
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run_session())
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
    #loop.close()



if __name__ == '__main__':
    main()


