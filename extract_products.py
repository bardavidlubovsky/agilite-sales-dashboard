import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import re
from datetime import datetime

base_url = "https://agilite.co.il"
start_url = "https://agilite.co.il/collections/all"
product_links = []
visited_pages = set()

while start_url and start_url not in visited_pages:
    visited_pages.add(start_url)
    try:
        response = requests.get(start_url, timeout=20)
        soup = BeautifulSoup(response.text, 'html.parser')
        cards = soup.find_all('product-card')
        for card in cards:
            handle = card.get('handle')
            link_tag = card.find('a', href=True)
            full_link = urljoin(base_url, link_tag['href']) if link_tag else None
            if full_link and handle:
                product_links.append({
                    "Product_Handle": handle,
                    "Product_Link": full_link
                })
        next_link_tag = soup.find("link", rel="next")
        if next_link_tag and next_link_tag.get("href"):
            start_url = urljoin(base_url, next_link_tag["href"])
        else:
            break
        time.sleep(1)
    except Exception:
        break

product_card_df = pd.DataFrame(product_links)
rows = []
failed_links = []

for _, row in product_card_df.iterrows():
    url = row['Product_Link']
    try:
        res = requests.get(url, timeout=30)
        product_blocks = re.findall(
            r'aco_allProducts\.set\(`([^`]+)`\s*,\s*(\{.*?\})\);',
            res.text,
            flags=re.DOTALL | re.UNICODE
        )

        if not product_blocks:
            failed_links.append({"Product_Handle": row.get('Product_Handle'), "Product_Link": url})
            continue

        for product_handle, block in product_blocks:
            general_available = re.search(r'availableForSale\s*:\s*(true|false)', block)
            product_id_match = re.search(r'\n\s*id\s*:\s*["\'](\d{8,})["\']', block)
            product_id_clean = product_id_match.group(1) if product_id_match else None

            price_range_match = re.search(
                r'priceRange\s*:\s*\{\s*maxVariantPrice\s*:\s*\{\s*amount\s*:\s*`?([\d.,]+)`?\s*\},\s*minVariantPrice\s*:\s*\{\s*amount\s*:\s*`?([\d.,]+)`?',
                block
            )
            max_price_clean = price_range_match.group(1).replace(',', '') if price_range_match else None
            min_price_clean = price_range_match.group(2).replace(',', '') if price_range_match else None

            published_at = re.search(r'publishedAt\s*:\s*`(.*?)`', block)
            tags_match = re.search(r'tags\s*:\s*\[(.*?)\]', block, re.DOTALL)
            title_match = re.search(r'title\s*:\s*`(.*?)`[\s\S]*?variants\s*:', block)

            tags = ''
            if tags_match:
                tag_list = [t.strip().strip('"').strip("'") for t in tags_match.group(1).split(',')]
                tags = ', '.join(tag_list)

            variant_blocks = re.findall(
                r'\{\s*availableForSale\s*:\s*(true|false).*?quantityAvailable\s*:\s*(-?\d+).*?price\s*:\s*\{\s*amount\s*:\s*`?([\d.,]+)`,\s*currencyCode\s*:\s*`?(\w+)`.*?title\s*:\s*`(.*?)`',
                block,
                re.DOTALL
            )

            for variant in variant_blocks:
                variant_available, qty, price_amount, currency, variant_title = variant
                price_amount_clean = price_amount.replace(',', '')

                rows.append({
                    "Product_Page": url,
                    "Product_Handle": product_handle,
                    "Product_Title_HE": title_match.group(1) if title_match else None,
                    "Product_ID": product_id_clean,
                    "Product_Available": general_available.group(1) if general_available else None,
                    "Price_Max": max_price_clean,
                    "Price_Min": min_price_clean,
                    "Published_At": published_at.group(1) if published_at else None,
                    "Tags": tags,
                    "Variant_Title": variant_title,
                    "Variant_Available": variant_available,
                    "Variant_Qty": qty,
                    "Variant_Price": price_amount_clean,
                    "Variant_Currency": currency
                })
    except Exception as e:
        failed_links.append({
            "Product_Handle": row.get('Product_Handle'),
            "Product_Link": url,
            "Error": str(e)
        })

df = pd.DataFrame(rows)
df["Price_Max"] = df["Price_Max"].astype(float)
df["Price_Min"] = df["Price_Min"].astype(float)
df["Variant_Price"] = df["Variant_Price"].astype(float)
df["Product_Available"] = df["Product_Available"].map({'true': True, 'false': False})
df["Variant_Available"] = df["Variant_Available"].map({'true': True, 'false': False})
df["Variant_Qty"] = pd.to_numeric(df["Variant_Qty"], errors='coerce').astype("Int64")
df["Published_At"] = pd.to_datetime(df["Published_At"], errors='coerce', utc=True)
df["Published_At"] = df["Published_At"].dt.tz_convert("Asia/Jerusalem")
df["Published_Date"] = df["Published_At"].dt.date
df["Published_Time"] = df["Published_At"].dt.time
df["Tags_List"] = df["Tags"].apply(lambda x: [tag.strip() for tag in x.split(",")] if pd.notnull(x) else [])

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
df.to_csv(f"data/agilite_products_{timestamp}.csv", index=False)
df.to_csv("data/latest_products.csv", index=False)

if failed_links:
    failed_df = pd.DataFrame(failed_links)
    failed_df.to_csv(f"agilite_failed_links_{timestamp}.csv", index=False)

# trigger GitHub Pages publish
