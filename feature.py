import numpy as np
import pandas as pd
import plotly.express as px
import plotly.io as pio 
pio.renderers.default = "browser"

from pprint import pprint
df = pd.read_parquet("data.parquet")
# pprint(dict(df.describe(include='all')))

print(df.corr(numeric_only=True))

# df["test"] = (np.abs(df["Outstanding_Debt"])**2 + np.abs(df["Monthly_Balance"])**2)**(1/10)
df["test"] = (np.abs(df["Delay_from_due_date"])**2 + np.abs(df["Num_Credit_Inquiries"])**2)**(1/5)

fig = px.imshow(
    df.corr(numeric_only=True),
    text_auto='.3f',  # Show values with 3 decimal places
    color_continuous_scale='Viridis',
    title='Correlation Heatmap',
    width=1000,  # Increase figure width
    height=800   # Increase figure height
)
# Decrease font size for x and y axis labels and tick labels
fig.update_xaxes(tickfont=dict(size=10), title_font=dict(size=12))
fig.update_yaxes(tickfont=dict(size=10), title_font=dict(size=12))
# Decrease font size for colorbar
fig.update_coloraxes(colorbar_tickfont=dict(size=10), colorbar_title_font=dict(size=12))
# Decrease font size for text annotations
fig.update_traces(textfont=dict(size=10))
fig.show()