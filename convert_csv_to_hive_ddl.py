from numpy.lib.arraysetops import unique
import pandas as pd
import numpy as np
import json


df = pd.read_csv('application_test.csv')  # All_POC_List.csv

print(df.dtypes)
a = list(df.columns)
print(a)
column_name_schema = []
unique_data_type = []
count = 0
for i in a:
    column_name_schema.append(i+" string,"+"\n")
    count = count + 1


print(column_name_schema)
print(count)
print(unique_data_type)


def append_multiple_lines(file_name, lines_to_append):
    # Open the file in append & read mode ('a+')
    with open(file_name, "a+") as file_object:
        for line in lines_to_append:
            file_object.write(line)


append_multiple_lines('sample.txt', column_name_schema)
