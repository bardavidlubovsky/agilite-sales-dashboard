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
df = df.groupby(['Product_Variant_ID', df['Snapshot_Timestamp'].dt.date], as_index=False).last()

df['qty_change'] = df.groupby('Product_Variant_ID')['Variant_Qty'].diff().fillna(0)
df['qty_sold'] = df['qty_change'].apply(lambda x: -x if x < 0 else 0)

df['Weekday'] = df['Snapshot_Timestamp'].dt.day_name()
df['Month'] = df['Snapshot_Timestamp'].dt.month_name()

base_info = df.groupby('Product_Variant_ID', as_index=False).agg({
    'Product_ID': 'last',
    'Variant_Title': 'last',
    'Product_Handle': 'last',
    'Product_Title_HE': 'last',
    'Product_Available': 'last',
    'Variant_Available': 'last'
})

weekday_sales = (
    df.groupby(['Product_Variant_ID', 'Weekday'])['qty_sold']
    .sum().reset_index()
)

month_sales = (
    df.groupby(['Product_Variant_ID', 'Month'])['qty_sold']
    .sum().reset_index()
)

weekday_df = pd.merge(base_info, weekday_sales, on='Product_Variant_ID', how='left')
weekday_df['Type'] = 'Weekday'
weekday_df.rename(columns={'Weekday': 'Time_Dimension', 'qty_sold': 'qty_sold_total'}, inplace=True)

month_df = pd.merge(base_info, month_sales, on='Product_Variant_ID', how='left')
month_df['Type'] = 'Month'
month_df.rename(columns={'Month': 'Time_Dimension', 'qty_sold': 'qty_sold_total'}, inplace=True)

final_df = pd.concat([weekday_df, month_df], ignore_index=True)
final_df.to_csv("data/sales_by_day_output.csv", index=False)
