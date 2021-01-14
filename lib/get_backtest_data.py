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
date = datetime.strptime('2019-11-19', '%Y-%m-%d')

# Convert to unix for the API
date_ms = unix_time_millisec(date)

# Get a current list of all the stock symbols for the NYSE
alpha = list(string.ascii_uppercase)

symbols = []

for each in alpha:
    url = 'http://eoddata.com/stocklist/NYSE/{}.htm'.format(each)
    resp = requests.get(url)
    site = resp.content
    soup = BeautifulSoup(site, 'html.parser')
    table = soup.find('table', {'class': 'quotes'})
    for row in table.findAll('tr')[1:]:
        symbols.append(row.findAll('td')[0].text.rstrip())
    
symbols_sliced = random.sample(symbols, 120)

# Remove the extra letters on the end
symbols_clean = []
# print(symbols)

for each in symbols_sliced:
    each = each.replace('.', '-')
    symbols_clean.append((each.split('-')[0]))

# Get the price history for each stock. This can take a while
consumer_key = 'BIFGL3BYYNDPGQRLVDA50OOH0OSXVIGR'

async def get_pricehistory(symbol, session):
    params = {
        'apikey': consumer_key,
        'periodType': 'month',
        'frequencyType': 'daily',
        'frequency': 1,
        'startDate': date_ms,
        'endDate': date_ms,
        'needExtendedHoursData': 'true'
        }
    url = r"https://api.tdameritrade.com/v1/marketdata/{}/pricehistory".format(symbol)
    try:
        response = await session.request(method='GET', url=url, params=params)
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
    open_price = response.get("candles", [{}])[0]['open']
    close_price = response.get("candles", [{}])[0]['close']
    volume = response.get("candles", [{}])[0]['volume']
    date = response.get("candles", [{}])[0]['datetime']
    return (
        symbol,
        open_price,
        close_price,
        volume,
        date
    )

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
        parsed_response = extract_responses(response)
        # print(f"Response: {json.dumps(parsed_response, indent=2)}")
        return parsed_response
    except Exception as err:
        print(f"Exception occurred: {err}")
        pass

async def run_session():
    async with ClientSession() as session:
        price_history_list = await asyncio.gather(*[run_program(symbol, session) for symbol in symbols_clean])
        
        #Turn results into Pandas DataFrame
        df = pd.DataFrame(list(price_history_list), columns=['symbol', 'open', 'close', 'volume', 'date'])
        df.astype({'symbol': 'str', 'open': 'float', 'close': 'float', 'volume': 'float', 'date': 'string'}).dtypes
        df['date']=pd.to_datetime(df['date'], unit='ms')
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        df = df.dropna()
        print(df.head())

        # Add to BigQuery
        client = bigquery.Client()
        dataset_id = "{}.equity_data".format(client.project)
        table_id = 'pricehistory_data_NYSE'

        # dataset = bigquery.Dataset(dataset_id)

        # dataset.location = "US"
        # dataset = client.create_dataset(dataset, timeout = 30)

        table_ref = "{}.{}".format(dataset_id,table_id)
                    
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.autodetect = True
        job_config.ignore_unknown_values = True

        job = client.load_table_from_dataframe(
                df,
                table_ref,
                location="US",
                job_config=job_config
                )
        job.result()

def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_session())
    #loop.close()



if __name__ == '__main__':
    main()


# for each in symbols_clean:
#     url = r"https://api.tdameritrade.com/v1/marketdata/{}/pricehistory".format(each)

#     # You can do whatever period/frequency you want
#     # This will grab the data for a single day
#     params = {
#         'apikey': consumer_key,
#         'periodType': 'month',
#         'frequencyType': 'daily',
#         'frequency': '1',
#         'startDate': date_ms,
#         'endDate': date_ms,
#         'needExtendedHoursData': 'true'
#         }

#     request = requests.get(
#         url=url,
#         params=params
#         )

#     data_list.append(request.json())
#     time.sleep(.5)



# for data in data_list:
#     try:
#         symbl_name  = data['symbol']
#     except KeyError:
#         symbl_name = np.nan
#     try:
#         for each in data['candles']:
#             symbl_l.append(symbl_name)
#             open_l.append(each['open'])
#             high_l.append(each['high'])
#             low_l.append(each['low'])
#             close_l.append(each['close'])
#             date_l.append(each['datetime'])
#     except KeyError:
#         pass

