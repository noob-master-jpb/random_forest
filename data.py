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


def to_months(text):
    try:
        text = str(text)
    except:
        pass
    if text is None:
        return None
    if not isinstance(text, str):
        return text
    try:
        data = text.split(" ")
        for i in data:
            i = i.strip()
        while '' in data:
            data.remove('')
        years = int(data[0])
        months = int(data[3])
        months += years*12
        return months
    except:
        return None


df["Credit_History_Age"] = df["Credit_History_Age"].apply(to_months)

df["Monthly_Balance"] = df.groupby("Customer_ID")["Monthly_Balance"].transform(lambda x: x.fillna(x.mean()))
df["Monthly_Balance"] = df["Monthly_Balance"].transform(lambda x: x.fillna(df["Monthly_Balance"].mean()))
df["Payment_of_Min_Amount"].fillna(0, inplace=True)



month_order = {
    'January': 1, 'February': 2, 'March': 3, 'April': 4,
    'May': 5, 'June': 6, 'July': 7, 'August': 8,
    'September': 9, 'October': 10, 'November': 11, 'December': 12
}
df['Month_num'] = df['Month'].map(month_order)
df['Credit_History_Age'] = df.groupby('Customer_ID')['Credit_History_Age'].transform(lambda group: group.interpolate(method='linear'))
df['Credit_History_Age'] = df.groupby('Customer_ID')['Credit_History_Age'].transform(lambda group: group.ffill().bfill())
df.dropna(subset=['Credit_History_Age'], inplace=True)
df["Changed_Credit_Limit"].fillna(0, inplace=True)

df['Outstanding_Debt'] = df['Outstanding_Debt'].fillna(df['Outstanding_Debt'].mean())

df.to_parquet("data.parquet", index=False)