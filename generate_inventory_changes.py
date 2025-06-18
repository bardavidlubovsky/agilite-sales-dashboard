import pandas as pd

df = pd.read_csv("data/historical_products.csv")
df['Snapshot_Timestamp'] = pd.to_datetime(df['Snapshot_Timestamp'])
df = df.sort_values(['Product_ID', 'Variant_Title', 'Snapshot_Timestamp'])

df['product_variant_id'] = df['Product_ID'].astype(str) + '_' + df['Variant_Title']
df['qty_change'] = df.groupby('product_variant_id')['Variant_Qty'].diff()

df['sales_only'] = df['qty_change'].apply(lambda x: -x if x < 0 else 0)
df['added_only'] = df['qty_change'].apply(lambda x: x if x > 0 else 0)

totals = df.groupby('product_variant_id').agg({
    'sales_only': 'sum',
    'added_only': 'sum'
}).round(2).rename(columns={
    'sales_only': 'Total_Sales',
    'added_only': 'Total_Added'
})

latest_rows = (
    df.sort_values('Snapshot_Timestamp')
    .drop_duplicates('product_variant_id', keep='last')
    .set_index('product_variant_id')
)

final_df = latest_rows.merge(totals, left_index=True, right_index=True)
final_df.reset_index(inplace=True)
final_df.insert(0, 'Product_Variant_ID', final_df['product_variant_id'])

final_df.to_csv("data/inventory_changes_output.csv", index=False)
