import requests
import json
import os
import pandas as pd
from tqdm import tqdm

# os.environ['cryptonews_api_key'] = 'YOUR_KEY'
cryptonews_api_key = os.getenv('cryptonews_api_key')

# Set the date range
date_range = pd.date_range(start='2021-01-01', end='2024-12-31')

# Loop through each date and request the data
all_data = []
for date in tqdm(date_range, desc="Fetching Data", unit="date"):
  date = date.strftime('%m%d%Y')
  paginated_url = f"https://cryptonews-api.com/api/v1?tickers=BTC&date={date}-{date}&items=100&page=1&token={cryptonews_api_key}"
  paginated_response = requests.get(paginated_url).json()
  paginated_data = paginated_response['data']
  all_data.extend(paginated_data)

  # If there are more than 1 page, loop through the pages
  if paginated_response['total_pages'] > 1:
    for page in range(2, paginated_response['total_pages'] + 1):
      paginated_url = f"https://cryptonews-api.com/api/v1?tickers=BTC&date={date}-{date}&items=100&page={page}&token={cryptonews_api_key}"
      paginated_response = requests.get(paginated_url).json()
      paginated_data = paginated_response['data']
      all_data.extend(paginated_data)

# Save the data to a parquet file
df = pd.DataFrame(all_data)
df.to_parquet("data/1.cryptonews.parquet", index=False)