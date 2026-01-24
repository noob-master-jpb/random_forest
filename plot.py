import numpy as np
import pandas as pd
import plotly.express as px
import plotly.io as pio 
pio.renderers.default = "browser"

from pprint import pprint
df = pd.read_parquet("data.parquet")


fig = px.violin(x=df["Credit_Score"], y=df["Monthly_Balance"],)
fig.show()