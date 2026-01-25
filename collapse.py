import numpy as np
import pandas as pd
import plotly.express as px
import plotly.io as pio 
pio.renderers.default = "browser"

from pprint import pprint
df = pd.read_parquet("data.parquet")

group = df.groupby("Customer_ID")
rows = []

for customer_id, data in group:
    row = {
        "Occupation": data["Occupation"].mode().iloc[0] if not data["Occupation"].mode().empty else np.nan,
        "Credit_Mix": data["Credit_Mix"].iloc[-1],
        "Credit_History_Age_last": data["Credit_History_Age"].iloc[-1],
        "Payment_of_Min_Amount_mode": data["Payment_of_Min_Amount"].mode().iloc[0] if not data["Payment_of_Min_Amount"].mode().empty else np.nan,
    }
    for col in ["Annual_Income", "Num_of_Loan", "Num_Bank_Accounts", "Num_Credit_Card"]:
        row[col] = data[col].iloc[0]
    for col in ["Num_Credit_Inquiries", "Delay_from_due_date", "Num_of_Delayed_Payment", "Outstanding_Debt", "Monthly_Balance"]:
        row[f"{col}_mean"] = data[col].mean()
        row[f"{col}_std"] = data[col].std()
        row[f"{col}_max"] = data[col].max()
        if col in ["Outstanding_Debt", "Monthly_Balance"]:
            row[f"{col}_last"] = data[col].iloc[-1]
            
    row["Credit_Score"] = data["Credit_Score"].iloc[-1]
    rows.append(row)

tdf = pd.DataFrame(rows)

# Fill std NaNs with 0
std_cols = [col for col in tdf.columns if col.endswith('_std')]
tdf[std_cols] = tdf[std_cols].fillna(0)

tdf.to_parquet("collapsed_data.parquet", index=False)
