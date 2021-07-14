import pandas as pd


def read_csv():
    df = pd.read_csv('pipeline/retailstore.csv')
    df2 = pd.read_csv('pipeline/retailstore.csv')
    return df, df2


a = read_csv()
print(type(a))
for i in range(len(a)):
    print(a[i].head())
