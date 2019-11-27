# File for moving all files from all_json into the Mongo Atlas cluster
# Only can fill up to 512 MB for the free tier of Atlas
# Make sure to change the connection string
# Requires the packages pymongo, pandas, and dnspython to execute
# Only run this on new Atlas clusters or when the specified db doesn't exist if doing full load

import pandas as pd
import pymongo
client = pymongo.MongoClient("mongodb+srv://user:userpassword@cluster0-lgn2s.gcp.mongodb.net/test?retryWrites=true&w=majority")

# These will create if the db and/or the collections don't exist
db = client['airbnb']
root = "C:/<change to your local path>"
listings_collection = db['listings'] # change this to use the <collection_name>
reviews_collection = db['reviews'] # change this to use the <collection_name>
file_index_listings = [78, 80, 81, 84, 86, 87, 88, 89]
file_index_reviews = [91]

# Loop through listings files
for iteration in file_index_listings:
    file_name = root + str(iteration)+"listings.json"
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
            dict.clear()
    print("File sent")

# Loop through reviews files
for iteration in file_index_reviews:
    file_name = root + str(iteration)+"reviews.json"
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
            dict.clear()
    print("File sent")

