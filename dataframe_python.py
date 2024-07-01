# polars script to
# load data from exchange API
# remove rows with None volumeUsd
# Convert percentTotalVolume, volumeUSD to Decimal
# Convert updated to timestamp with the get_utc_from_unit_time function
# bucket tradingPairs into 0-100, 100-500, 500+ range
# group by bucket and create count -> output
from decimal import Decimal

import pandas as pd

from native_python import (
    determine_bucket,
    extract_coincap_api,
    flatten_exchange_data,
    get_utc_from_unix_time,
)

url = "https://api.coincap.io/v2/exchanges"
raw_data = flatten_exchange_data(extract_coincap_api(url))

df = pd.DataFrame(raw_data)

# Remove rows with None volumeUSD
df = df[df["volumeUsd"].notna()]

# Convert percentTotalVolume, volumeUSD to Decimal
df["percentTotalVolume"] = df["percentTotalVolume"].apply(Decimal)
df["volumeUSD"] = df["volumeUsd"].apply(Decimal)
df["tradingPairs"] = df["tradingPairs"].apply(int)
df["updated"] = df["updated"].apply(get_utc_from_unix_time)

bucket_ranges = [100, 500]
df["bucket"] = df["tradingPairs"].apply(
    lambda x: determine_bucket(int(x), bucket_ranges)
)

# Group by bucket and create count
grouped_df = df.groupby("bucket").size().reset_index(name="count")
print(grouped_df)
