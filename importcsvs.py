#File for moving all files from all_json into the Mongo Atlas cluster

#Only need to grab the top 10ish files from all_json if you want because it already pushes to the 512 MB limit with those

# Make sure to change the connection string

import sys
import pandas as pd
import pymongo
import json
import os
client = pymongo.MongoClient(
        "mongodb+srv://user:userpassword@cluster0-lgn2s.gcp.mongodb.net/test?retryWrites=true&w=majority")

db = client.airbnb # change this to use .<database_name>
collection = db['listings'] # change this to use the '<collection_name>
file_count = 0

while file_count < 3:
    file_name  = "C:/users/ianpe/desktop/all_json/" + str(file_count)+"listings.json"
    df = pd.read_json(file_name)
    dict=[]
    file_count +=1
    count=0
    for index, row in df.iterrows():
        row_dict = row.to_dict()
        # print(dict)
        # print(row_dict)
        # print(count)
        if count < 100:
            dict.append(row_dict)
            count +=1
        else:
            dict.append(row_dict)
            count = 0
            collection.insert_many(dict)
            # print(dict)
            print("rows sent")
            dict.clear()



