import pytz
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from google.cloud import bigquery
#import pyarrow
from google.cloud import storage
import string
import time

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

    print(today_fmt)

if __name__ == "__main__":
    daily_equity_quotes("a","b")