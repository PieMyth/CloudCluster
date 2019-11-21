import os

import pandas as pd


fileList = []
keywordList = []
nameList = []
sizes = []

for root, dirs, files in os.walk('c:/users/ianpe/desktop/all_json'):
   for name in files:
    print(name)
    fileList.append(os.path.join(root, name))
    nameList.append(name)
    sizes.append(os.path.getsize(root+"/"+name)>>20)
print(nameList)

file1 = open("c:/users/ianpe/desktop/listings_json_details.txt","w")
index =0
for file in fileList:
    df = pd.read_json(file)
    scope = df.head(1)
    string = str(nameList[index]) +"\t\t"+ str(scope['host_location'].values[0])+ "\t\t" +str(sizes[index])+ "\n"
    print(string)
    file1.write(string)
    index += 1
file1.close()
