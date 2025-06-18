import pandas as pd

df = pd.read_csv("data/historical_products.csv")
df['Snapshot_Timestamp'] = pd.to_datetime(df['Snapshot_Timestamp'])
df = df.sort_values(['Product_ID', 'Variant_Title', 'Snapshot_Timestamp'])

df['product_variant_id'] = df['Product_ID'].astype(str) + '_' + df['Variant_Title']
df['qty_change'] = df.groupby('product_variant_id')['Variant_Qty'].diff()

added_df = df.copy()
added_df['added_qty'] = added_df.groupby('product_variant_id')['Variant_Qty'].diff()
added_total = (
    added_df[added_df['added_qty'] > 0]
    .groupby('product_variant_id')['added_qty']
    .sum()
    .round(2)
    .replace(-0.0, 0.0)
)

df['timestamp_str'] = df['Snapshot_Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
df = df.drop_duplicates(subset=['product_variant_id', 'timestamp_str'], keep='last')

pivot_qty_change = df.pivot(index='product_variant_id', columns='timestamp_str', values='qty_change')
pivot_qty_change = pivot_qty_change.fillna(0)
pivot_qty_change = pivot_qty_change.sort_index(axis=1)

latest_rows = (
    df.sort_values('Snapshot_Timestamp')
    .drop_duplicates('product_variant_id', keep='last')
    .set_index('product_variant_id')
)

final_df = pivot_qty_change.merge(latest_rows, left_index=True, right_index=True)
final_df.reset_index(inplace=True)
final_df.insert(0, 'Product_Variant_ID', final_df['product_variant_id'])

qty_cols = [col for col in final_df.columns if col[:4].isdigit()]
final_df['Total_Sales'] = final_df[qty_cols].apply(lambda row: round(-row[row < 0].sum(), 2), axis=1)
final_df['Total_Sales'] = final_df['Total_Sales'].replace(-0.0, 0.0)
final_df['Total_Added'] = final_df['product_variant_id'].map(added_total).fillna(0.0)

final_df.to_csv("data/inventory_changes_output.csv", index=False)
