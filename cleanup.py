#File for converting all the csvs from the all_files into jsons in all_json
#csv_path = 'C:/users/ianpe/desktop/load_files_test/load_files'
import os
import pandas as pd
count = 0
path, dirs, files = next(os.walk('C:/users/ianpe/desktop/load_files_test/load_files'))
file_count = len(files)
print(file_count)
# file_index_listings = [82, 84, 86, 87, 88, 89]
file_index_listings = [78]
file_index_reviews = [91]
for cnt in file_index_listings:
    source_file= 'C:/users/ianpe/desktop/all_files/' + str(cnt)+"listings.csv"
    dest_file = 'C:/users/ianpe/desktop/listings_json_cleaned/' + str(count) + "listings.json"
    csv_file = pd.DataFrame(pd.read_csv(source_file, sep = ",", header = 0, index_col = False))
    csv_file['price'] = csv_file['price'].str.replace('$','').str.replace(',','').astype(float)
    # print(csv_file[['price']])
    csv_file['zipcode'] = csv_file['zipcode'].replace(r'\D','')
    csv_file.to_json(dest_file, orient = "records", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None)
    count +=1