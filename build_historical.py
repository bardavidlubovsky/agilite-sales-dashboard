import pandas as pd
import glob
import os
import re
from collections import defaultdict

historical_path = "data/historical_products.csv"
if os.path.exists(historical_path):
    os.remove(historical_path)

files = glob.glob("data/agilite_products_*.csv")
files = [f for f in files if "latest" not in f]

daily_files = defaultdict(list)

for file in files:
    filename = os.path.basename(file)
    match = re.search(r"agilite_products_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2})\.csv", filename)
    if match:
        date_str = match.group(1)
        time_str = match.group(2)
        full_timestamp = f"{date_str}_{time_str}"
        daily_files[date_str].append((file, full_timestamp))

selected_files = []
for date, file_list in daily_files.items():
    file_list.sort(key=lambda x: x[1])  
    selected_files.append(file_list[0])  

selected_files.sort(key=lambda x: x[1])

all_data = []
for file, timestamp_str in selected_files:
    df = pd.read_csv(file)
    df["Snapshot_Timestamp"] = pd.to_datetime(timestamp_str, format="%Y-%m-%d_%H-%M")
    all_data.append(df)

if all_data:
    historical_df = pd.concat(all_data, ignore_index=True)
    historical_df.to_csv(historical_path, index=False)
    print(f"Saved {len(historical_df)} rows to historical_products.csv")
else:
    print("No valid data found.")
