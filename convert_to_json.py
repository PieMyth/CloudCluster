#File for converting all the csvs from the all_files into jsons in all_json
csv_path = 'C:/users/ianpe/desktop/all_files'
json_path = 'C:/users/ianpe/desktop/all_json'
import os
import pandas as pd
count = 0
path, dirs, files = next(os.walk('C:/users/ianpe/desktop/all_files'))
file_count = len(files)
print(file_count)
while count<file_count:
    source_file= 'C:/users/ianpe/desktop/all_files/' + str(count)+"listings.csv"
    dest_file = 'C:/users/ianpe/desktop/all_listings_json_cleaned/' + str(count) + "listings.json"

    csv_file = pd.DataFrame(pd.read_csv(source_file, sep = ",", header = 0, index_col = False))
    csv_file['price'] = csv_file['price'].str.replace('$','').str.replace(',','').astype(float)
    csv_file['weekly_price'] = csv_file['weekly_price'].str.replace('$','').str.replace(',','').astype(float)
    csv_file['security_deposit'] = csv_file['security_deposit'].str.replace('$','').str.replace(',','').astype(float)
    csv_file['cleaning_fee'] = csv_file['cleaning_fee'].str.replace('$','').str.replace(',','').astype(float)
    csv_file['extra_people'] = csv_file['extra_people'].str.replace('$','').str.replace(',','').astype(float)
    csv_file['host_response_rate'] = csv_file['host_response_rate'].str.replace('%','').str.replace(',','').astype(float)
    csv_file['zipcode'] = csv_file['zipcode'].replace(r'\D','')

    csv_file.to_json(dest_file, orient = "records", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None)
    count +=1