from numpy.lib.arraysetops import unique
import pandas as pd
import numpy as np
import json


df = pd.read_csv('application_test.csv')  # All_POC_List.csv

print(df.dtypes)
a = list(df.columns)
print(a)
column_name = []
unique_data_type = []
count = 0
for i in a:

    if np.object == df.dtypes[i]:
        column_name.append(i+" VARCHAR(255) DEFAULT NULL,"+"\n")
        count = count+1
    elif np.int64 == df.dtypes[i]:
        column_name.append(i+" int,"+"\n")
        count = count+1
    elif np.float64 == df.dtypes[i]:
        column_name.append(i+" FLOAT(20,10),"+"\n")
        count = count+1

    if df[i].dtype not in unique_data_type:
        unique_data_type.append(df[i].dtype)


print(column_name)
print(count)
print(unique_data_type)


def append_multiple_lines(file_name, lines_to_append):
    # Open the file in append & read mode ('a+')
    with open(file_name, "a+") as file_object:
        for line in lines_to_append:
            file_object.write(line)


append_multiple_lines('sample.txt', column_name)
