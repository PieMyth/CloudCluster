import csv,json
csv_path = 'C:/users/ianpe/desktop/0listings.csv'
json_path = 'C:/users/ianpe/desktop/0listings.json'

import pandas as pd
csv_file = pd.DataFrame(pd.read_csv('C:/users/ianpe/desktop/0listings.csv', sep = ",", header = 0, index_col = False))
csv_file.to_json('C:/users/ianpe/desktop/0listings.json', orient = "records", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None)