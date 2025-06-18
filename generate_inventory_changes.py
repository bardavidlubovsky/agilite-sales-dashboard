import pandas as pd

df = pd.read_csv("data/historical_products.csv")
df['Snapshot_Timestamp'] = pd.to_datetime(df['Snapshot_Timestamp'])

df = df.groupby(['Product_ID', 'Variant_Title', 'Snapshot_Timestamp'], as_index=False).agg({
    'Variant_Qty': 'sum',
    'Variant_Price': 'last',
    'Product_Handle': 'last',
    'Product_Title_HE': 'last'
})

df = df.sort_values(['Product_ID', 'Variant_Title', 'Snapshot_Timestamp'])
df['product_variant_id'] = df['Product_ID'].astype(str) + '_' + df['Variant_Title']
df['qty_change'] = df.groupby('product_variant_id')['Variant_Qty'].diff()
df['Total_Sales'] = df['qty_change'].apply(lambda x: -x if x < 0 else 0)
df['Total_Added'] = df['qty_change'].apply(lambda x: x if x > 0 else 0)

agg_totals = df.groupby(['Product_ID', 'Variant_Title']).agg({
    'Total_Sales': 'sum',
    'Total_Added': 'sum',
    'Variant_Price': 'last',
    'Product_Handle': 'last',
    'Product_Title_HE': 'last'
}).round(2).reset_index()

agg_totals['Total_Sales'] = agg_totals['Total_Sales'].replace(-0.0, 0.0)
agg_totals['Total_Added'] = agg_totals['Total_Added'].replace(-0.0, 0.0)

latest_qty = (
    df.sort_values('Snapshot_Timestamp')
    .drop_duplicates(subset=['Product_ID', 'Variant_Title'], keep='last')
    [['Product_ID', 'Variant_Title', 'Variant_Qty']]
)

final_df = pd.merge(agg_totals, latest_qty, on=['Product_ID', 'Variant_Title'], how='left')
final_df.insert(0, 'Product_Variant_ID', final_df['Product_ID'].astype(str) + '_' + final_df['Variant_Title'])

final_df['Revenue'] = final_df['Total_Sales'] * final_df['Variant_Price']
final_df['Inventory_Value'] = final_df['Variant_Qty'] * final_df['Variant_Price']
final_df['Refund_Liability'] = final_df.apply(
    lambda row: row['Variant_Qty'] * row['Variant_Price'] if row['Variant_Price'] < 0 else 0,
    axis=1
)

final_df.to_csv("data/inventory_changes_output.csv", index=False)
