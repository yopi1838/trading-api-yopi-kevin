#Import dependencies
import pandas as pd
import numpy as np
from scipy import stats
import requests
import time
from datetime import datetime
import pytz
from google.cloud import bigquery
from google.cloud import storage
import pyarrow
import alpaca_trade_api as trade_api

def trade_bot():
    #Get API key from Cloud Storage
    storage_client = storage.client()
    bucket = storage_client.get_bucket('trading-api-ky')
    blob = bucket.blob('secret-key.txt')
    api_key = blob.download_as_string()

    # Check if market was open today
    today = datetime.today().astimezone(pytz.timezone("Asia/Tokyo"))
    today_fmt = today.strftime('%Y-%m-%d')

    market_url = 'https://api.tdameritrade.com/v1/marketdata/EQUITY/hours'

    params = {
        'apikey': api_key,
        'date': today_fmt
    }

    request = requests.get(
        url = market_url,
        params=params
        ).json()

    try:
        if request['equity']['EQ']['isOpen'] is True:
            # call BigQuery
            client = bigquery.client()

            # Load historical stock data from BigQuery
            sql_hist = """
                SELECT
                    symbol,
                    closePrice,
                    date
                FROM
                    kevin-yopi-trading-api.equity_data.daily_quote_data
                """
            
            df = client.query(sql_hist).to_dataframe()

            # Convert date column to datetime
            df['date'] = pd.to_datetime(df['date'])

            #Sort date for momentum calculation
            df = df.sort_values(by='date').reset_index(drop=True)

            
