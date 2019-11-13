#File for converting all the csvs from the all_files into jsons in all_json
# csv_path = 'C:/users/ianpe/desktop/all_files'
# json_path = 'C:/users/ianpe/desktop/all_json'
import os
import pandas as pd
count = 0
path, dirs, files = next(os.walk('C:/users/ianpe/desktop/reviews_csvs'))
file_count = len(files)
print(file_count)
while count<file_count:
    source_file= 'C:/users/ianpe/desktop/reviews_csvs/' + str(count)+"reviews.csv"
    dest_file = 'C:/users/ianpe/desktop/reviews_json/' + str(count) + "reviews.json"
    csv_file = pd.DataFrame(pd.read_csv(source_file, sep = ",", header = 0, index_col = False))
    csv_file.to_json(dest_file, orient = "records", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None)
    count +=1