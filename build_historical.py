import pandas as pd
import glob
import os
import re

# חיפוש כל הקבצים שמתאימים לתבנית (למעט latest)
files = glob.glob("data/agilite_products_*.csv")
files = [f for f in files if "latest" not in f]

all_data = []

for file in files:
    filename = os.path.basename(file)
    match = re.search(r"agilite_products_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2})\.csv", filename)
    if not match:
        continue
    timestamp_str = match.group(1)
    
    df = pd.read_csv(file)
    df["Snapshot_Timestamp"] = pd.to_datetime(timestamp_str, format="%Y-%m-%d_%H-%M")
    all_data.append(df)

# אם אין קבצים, לא ניצור קובץ חדש
if all_data:
    historical_df = pd.concat(all_data, ignore_index=True)
    historical_df.to_csv("data/historical_products.csv", index=False)
