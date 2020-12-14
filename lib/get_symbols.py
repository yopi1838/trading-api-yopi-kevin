import csv
import os

def get_symbols():
    file_location = r"C:\Users\Yopi-CEC\Desktop\programming\trading-api-yopi\data"
    with open(os.path.join(file_location,"nasdaqlisted.txt"), "r") as f:
        first_column = [row[0] for row in csv.reader(f, delimiter="|")]
        #print(first_column[1])

if __name__ == "__main__":
    get_symbols()