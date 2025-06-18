import pandas as pd

df = pd.read_csv("data/historical_products.csv")
df['Snapshot_Timestamp'] = pd.to_datetime(df['Snapshot_Timestamp'])

df = df.groupby(['Product_ID', 'Variant_Title', 'Snapshot_Timestamp'], as_index=False).agg({
    'Variant_Qty': 'sum',
    'Variant_Price': 'last',
    'Product_Handle': 'last',
    'Product_Title_HE': 'last',
    'Product_Available': 'last',
    'Variant_Available': 'last'
})

df = df.sort_values(['Product_ID', 'Variant_Title', 'Snapshot_Timestamp'])
df['Product_Variant_ID'] = df['Product_ID'].astype(str) + '_' + df['Variant_Title']
df = df.groupby(['Product_Variant_ID', df['Snapshot_Timestamp'].dt.date], as_index=False).first()

df['qty_change'] = df.groupby('Product_Variant_ID')['Variant_Qty'].diff().fillna(0)
df['qty_sold'] = df['qty_change'].apply(lambda x: -x if x < 0 else 0)

df['Weekday'] = df['Snapshot_Timestamp'].dt.day_name()
df['Month'] = df['Snapshot_Timestamp'].dt.month_name()

weekdays_order = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
weekday_totals = df.groupby(['Product_Variant_ID', 'Weekday'])['qty_sold'].sum().unstack().reindex(columns=weekdays_order, fill_value=0).reset_index()

months_order = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
]
month_totals = df.groupby(['Product_Variant_ID', 'Month'])['qty_sold'].sum().unstack().reindex(columns=months_order, fill_value=0).reset_index()

base_info = df.groupby('Product_Variant_ID', as_index=False).agg({
    'Product_ID': 'last',
    'Variant_Title': 'last',
    'Product_Handle': 'last',
    'Product_Title_HE': 'last',
    'Product_Available': 'last',
    'Variant_Available': 'last'
})

final_df = pd.merge(base_info, weekday_totals, on='Product_Variant_ID', how='left')
final_df = pd.merge(final_df, month_totals, on='Product_Variant_ID', how='left')

final_df.to_csv("data/sales_by_day_output.csv", index=False)
