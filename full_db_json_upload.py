#File for moving all files from all_json into the Mongo Atlas cluster

#Only need to grab the top 10ish files from all_json if you want because it already pushes to the 512 MB limit with those

# Make sure to change the connection string

import sys
import pandas as pd
import pymongo
import json
import os

client = pymongo.MongoClient("mongodb+srv://user:userpassword@cluster0-lgn2s.gcp.mongodb.net/test?retryWrites=true&w=majority")

# dbs = client.list_database_names()
# print(dbs)
# db = client.airbnb # change this to use .<database_name>
db = client['airbnb']
listings_collection = db['listings'] # change this to use the '<collection_name>
reviews_collection = db['reviews'] # change this to use the '<collection_name>
file_index_listings = [82, 84, 86, 87, 88, 89]
file_index_reviews = [91]

for iteration in file_index_listings:
    file_name  = "C:/users/ianpe/desktop/loads/" + str(iteration)+"listings.json"
    print(file_name)
    df = pd.read_json(file_name)
    dict = []
    count=0
    for index, row in df.iterrows():
        row_dict = row.to_dict()
        if count < 100:
            dict.append(row_dict)
            count += 1
        else:
            dict.append(row_dict)
            count = 0
            listings_collection.insert_many(dict)
            print("rows sent")
            dict.clear()

for iteration in file_index_reviews:
    file_name  = "C:/users/ianpe/desktop/loads/" + str(iteration)+"reviews.json"
    df = pd.read_json(file_name)
    dict = []
    count=0
    for index, row in df.iterrows():
        row_dict = row.to_dict()
        if count < 1000:
            dict.append(row_dict)
            count += 1
        else:
            dict.append(row_dict)
            count = 0
            reviews_collection.insert_many(dict)
            print("rows sent")
            dict.clear()


