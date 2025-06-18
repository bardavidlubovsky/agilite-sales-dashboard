import pandas as pd

df = pd.read_csv("data/historical_products.csv")
df['Snapshot_Timestamp'] = pd.to_datetime(df['Snapshot_Timestamp'])

df = df.groupby(['Product_ID', 'Variant_Title', 'Snapshot_Timestamp'], as_index=False).agg({
    'Variant_Qty': 'sum',
    'Variant_Price': 'last',
    'Product_Handle': 'last',
    'Product_Title_HE': 'last',
    'Product_Available': 'last'
})

df = df.sort_values(['Product_ID', 'Variant_Title', 'Snapshot_Timestamp'])
df['Product_Variant_ID'] = df['Product_ID'].astype(str) + '_' + df['Variant_Title']

df = df.groupby(['Product_Variant_ID', df['Snapshot_Timestamp'].dt.date], as_index=False).first()

df['qty_change'] = df.groupby('Product_Variant_ID')['Variant_Qty'].diff().fillna(0)
df['qty_sold'] = df['qty_change'].apply(lambda x: -x if x < 0 else 0)
df['Sale_Date'] = pd.to_datetime(df['Snapshot_Timestamp']).dt.date
df['Weekday'] = pd.to_datetime(df['Snapshot_Timestamp']).dt.day_name()

pivot_by_date = df.pivot_table(
    index=['Product_Variant_ID', 'Product_ID', 'Variant_Title', 'Product_Handle', 'Product_Title_HE', 'Product_Available'],
    columns='Sale_Date',
    values='qty_sold',
    aggfunc='sum'
).fillna(0).reset_index()

weekday_totals = df.groupby(['Product_Variant_ID', 'Weekday'])['qty_sold'].sum().unstack(fill_value=0).reset_index()

final_df = pd.merge(pivot_by_date, weekday_totals, on='Product_Variant_ID', how='left')
final_df.to_csv("data/sales_by_day_output.csv", index=False)
