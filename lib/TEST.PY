import csv
import os.path

my_path = os.path.abspath(os.path.dirname(__file__))
path = os.path.join(my_path, "../data/test.csv")
with open(path) as f:
    test = list(csv.reader(f))
print(test)