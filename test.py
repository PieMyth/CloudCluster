import csv
import json

output = {}
count = 0

with open('c:/users/ianpe/desktop/countries.txt') as json_file:
    data = json.load(json_file)
    for p in data['countries']:
        print(p['argentina'])


# with open('c:/users/ianpe/desktop/listings.csv', encoding='utf8') as csv_file:
#     for row in csv.DictReader(csv_file):
#         if row['country'] in output:
#             country = output[row['country']]
#         else:
#             country = { "state": []}
#             output[row['country']] = country
#         if row['state'] not in country['state']:
#             print("good")
#             country['state'].append(row['state'])
#         # count+=1
#
#
# print(json.dumps(output, indent=4))
# with open('data.txt', 'w') as outfile:
#     outfile.write(json.dumps(output, indent=4))

# data = {}
# data['countries']=[]
# data['countries'].append({'argentina':'7748'})
# with open('c:/users/ianpe/desktop/countries.txt','w') as outfile:
#     json.dump(data,outfile)

