import pandas as pd
import polars as pl
import plotly.express as px
import plotly.io as pio
from pprint import pprint
pio.renderers.default = "browser"

df = pd.read_parquet("data.parquet")
# pprint(dict(df.describe(include="all")))

def test(input):
    input = input.strip("_")
    return int(input)

df["Age"] = df["Age"].apply(test)

df.to_parquet("data.parquet")