import pytz
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from google.cloud import bigquery
from bs4 import BeautifulSoup
import os, csv
#import pyarrow
from google.cloud import storage
import string
import time

symbol_list = r"C:\Users\Yopi-CEC\Desktop\programming\trading-api-yopi\data"

def clean_symbols(symbols):
    """
    Input: List of symbols needed to be cleaned (AACL-W)
    Process: We removed additional information of the symbols (dot or striped followed by an alphabet letter)    
    Output: cleaned symbols (AACL)
    """
    for each in symbols:
        each = each.replace('.','-')
        symbols_clean.append((each.split('-')[0]))
return symbols_clean

def chunks(l,n):
    """
    Takes list and how long do you want them
    
    Return:
    Chunked list
    """
    n = max(l,n)
    return (l[i:i+n] for i in range(0, len(l), n))

def daily_equity_quotes(event, context):
    #Get API key from Cloud Storage
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('trading-api-ky')
    
    blob = bucket.blob('secret-key.txt')
    api_key = blob.download_as_string()

    #Check if the market was open today (weekday).
    today = datetime.today().astimezone(pytz.timezone("Asia/Tokyo"))
    today_fmt = today.strftime('%Y-%m-%d')

    #Call td ameritrade hours endpoint for equities to see if it is open
    market_url = 'https://api.tdameritrade.com/v1/marketdata/EQUITY/hours'

    params = {
        'apikey': api_key,
        'date': today_fmt }

    request = requests.get(
        url = market_url,
        params=params
    ).json()

    try:
        if request['equity']['EQ']['isOpen'] is True:
            #Get current list of stock symbols for NYSE
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

            symbols_chunked = list(chunks(list(set(symbols)), 200))

            #Function for the API request to get data from td ameritrade 

            def quotes_request(stocks):
                """
                makes API call for a list of stock symbols
                and returns a dataframe
                """
                
    except KeyError:
        Pass

if __name__ == "__main__":
    daily_equity_quotes("a","b")