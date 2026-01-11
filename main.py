import pandas as pd
import polars as pl
import plotly.express as px
import plotly.io as pio
from pprint import pprint
pio.renderers.default = "browser"


df = pd.read_csv("dataset/train.csv",low_memory=False)


df = df[['Customer_ID','Month', 'Age', 'Occupation', 'Annual_Income', 'Monthly_Inhand_Salary',
       'Num_Bank_Accounts', 'Num_Credit_Card', 'Num_of_Loan',
       'Delay_from_due_date', 'Num_of_Delayed_Payment', 'Changed_Credit_Limit',
       'Num_Credit_Inquiries', 'Credit_Mix', 'Outstanding_Debt',
       'Credit_Utilization_Ratio', 'Credit_History_Age',
       'Payment_of_Min_Amount', 'Total_EMI_per_month',
       'Amount_invested_monthly', 'Monthly_Balance',"Credit_Score"]]
def to_float(x):
    if x is None:
        return None
    if x is str:
        x = x.strip("_")
    try:
        return float(x)
    except:
        return None
    
cols = ["Age", "Annual_Income", "Num_of_Loan","Num_of_Delayed_Payment",
        "Changed_Credit_Limit",'Outstanding_Debt',"Amount_invested_monthly","Amount_invested_monthly","Monthly_Balance"]
for col in cols:
    df[col] = df[col].apply(to_float)
    
    
def to_bool(x):
    if x is None:
        return None
    if x in 'Yes':
        return 1
    if x in 'No':
        return 0
    return None
df["Payment_of_Min_Amount"] = df["Payment_of_Min_Amount"].apply(to_bool)

df = df[(df["Age"]>=18) & (df["Age"]<=60)]
df = df[(df["Annual_Income"]<=6000000)]
df = df[(df["Monthly_Inhand_Salary"]<=15000)]
df = df[(df["Num_Bank_Accounts"]<=20)]
df = df[(df["Num_Credit_Card"]<=30)]
df = df[(df["Num_of_Loan"]<=100) & (df["Num_of_Loan"]>=0)]
df = df[(df["Num_of_Delayed_Payment"]<=300)]
df = df[(df["Num_Credit_Inquiries"]<=200)]
df = df[(df["Total_EMI_per_month"]<=500)&(df["Num_of_Delayed_Payment"]>=0)]
df = df[(df["Amount_invested_monthly"]<=1000)]









df.to_parquet("data.parquet", index=False)