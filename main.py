import pandas as pd
import polars as pl
import plotly.express as px
import plotly.io as pio
from pprint import pprint
pio.renderers.default = "browser"

df = pd.read_parquet("data.parquet")
# pprint(dict(df.describe(include="all")))



c = 0
df = df["Age"]
for i in df:
    if type(i) is not int:
        c += 1
print(c)
# print(type(df[0]))
