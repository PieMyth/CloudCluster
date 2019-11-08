import ijson
filename = "C:/users/ianpe/desktop/0listings.json"
import pandas as pd
# df = pd.read_json ("C:/users/ianpe/desktop/0listings.json")
# data_top = df.head()
#
# # display
# print(data_top

data = pd.read_csv("C:/users/ianpe/desktop/0listings.csv")

# iterating the columns
for col in data.columns:
    print(col)