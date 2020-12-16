import csv
import os
from google.cloud import storage

def get_symbols():
    #Get API key from Cloud Storage
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('trading-api-ky')
    
    #Get Symbol Lists
    symbol_blob = bucket.blob('nasdaqlisted.txt')
    symbol_list = symbol_blob.download_to_filename("gcp")
    
    with open(os.path.join(symbol_list,"nasdaqlisted.txt"), "r") as f:
        first_column = [row[0] for row in csv.reader(f, delimiter="|")]
        print(first_column[1])

if __name__ == "__main__":
    get_symbols()