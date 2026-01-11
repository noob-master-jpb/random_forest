import pandas as pd
import polars as pl
import plotly.express as px
import plotly.io as pio
from pprint import pprint
pio.renderers.default = "browser"

df = pd.read_parquet("data.parquet")
# pprint(dict(df.describe(include="all")))
# df2 = pd.read_csv("dataset/train.csv",low_memory=False)
def filter_int(input):
    if input is None:
        return None
    input = input.strip("_")
    try:
        return float(input)
    except:
        return None

def convert_to_binary(input):
    if input == "Yes":
        return 1
    elif input == "No":
        return 0
    else:
        return None



df = df[(df["Age"] >= 18) & (df["Age"] <= 60)]
df = df[(df["Num_Bank_Accounts"] <=10)]
df = df[(df["Total_EMI_per_month"] <=200)]

# print(df_filtered.describe(include="all"))
# col = "Payment_of_Min_Amount"
# df[col] = df2[col].apply(test2)

df.to_parquet("data.parquet")