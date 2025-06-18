import pandas as pd

df = pd.read_csv("data/historical_products.csv")
df['product_variant_id'] = df['Product_ID'].astype(str) + '_' + df['Variant_Title']
df['Snapshot_Timestamp'] = pd.to_datetime(df['Snapshot_Timestamp'])
df = df.sort_values(['product_variant_id', 'Snapshot_Timestamp'])

latest_qty = (
    df.sort_values('Snapshot_Timestamp')
    .drop_duplicates('product_variant_id', keep='last')
    .set_index('product_variant_id')[['Product_ID', 'Variant_Title', 'Variant_Qty']]
)

df['qty_change'] = df.groupby('product_variant_id')['Variant_Qty'].diff()
df['timestamp_str'] = df['Snapshot_Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

# ✅ הסרת כפילויות על אותו product_variant_id + timestamp
df = df.drop_duplicates(subset=['product_variant_id', 'timestamp_str'], keep='last')

pivot_qty_change = df.pivot(index='product_variant_id', columns='timestamp_str', values='qty_change')
pivot_qty_change = pivot_qty_change.fillna(0)
pivot_qty_change = pivot_qty_change.sort_index(axis=1)

pivot_qty_change = pivot_qty_change.merge(latest_qty, left_index=True, right_index=True)

cols = ['Product_ID', 'Variant_Title', 'Variant_Qty'] + [
    col for col in pivot_qty_change.columns if col not in ['Product_ID', 'Variant_Title', 'Variant_Qty']
]
pivot_qty_change = pivot_qty_change[cols]

pivot_qty_change['Total_Sales'] = pivot_qty_change.loc[:, pivot_qty_change.columns.str.match(r'\d{4}-\d{2}-\d{2}')].apply(
    lambda row: -row[row < 0].sum(), axis=1
)

pivot_qty_change.reset_index(drop=True, inplace=True)
pivot_qty_change.to_csv("data/inventory_changes_output.csv", index=False)
