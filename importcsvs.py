import sys
import pandas as pd
import pymongo
import json
import os
client = pymongo.MongoClient(
        "mongodb+srv://user:userpassword@cluster0-lgn2s.gcp.mongodb.net/test?retryWrites=true&w=majority")

db = client.airbnb
collection = db['airbnb']
with open("C:/users/ianpe/desktop/0listings.json") as f:
    file_data = json.load(f)
print(file_data)
# use collection_currency.insert(file_data) if pymongo version < 3.0
collection.insert_one(file_data)