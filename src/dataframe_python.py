# polars script to
# load data from exchange API
# remove rows with None volumeUsd
# Convert percentTotalVolume, volumeUSD to Decimal
# Convert updated to timestamp with the get_utc_from_unit_time function
# bucket tradingPairs into 0-100, 100-500, 500+ range
# group by bucket and create count -> output
from decimal import Decimal

import pandas as pd

from native_python import (determine_bucket, extract_coincap_api,
                           flatten_exchange_data, get_utc_from_unix_time)


def clean_exchange_data(df):
    # Remove rows with None volumeUSD
    df = df[df["volumeUsd"].notna()]
    # Convert percentTotalVolume, volumeUSD to Decimal
    df["percentTotalVolume"] = df["percentTotalVolume"].apply(Decimal)
    df["volumeUSD"] = df["volumeUsd"].apply(Decimal)
    df["tradingPairs"] = df["tradingPairs"].apply(int)
    df["updated"] = df["updated"].apply(get_utc_from_unix_time)
    return df


def bucket_exchange_data(df, column_name, bucket_ranges):
    df["bucket"] = df[column_name].apply(
        lambda x: determine_bucket(int(x), bucket_ranges)
    )
    return df


def transform_exchange_data(url_data):
    raw_data = flatten_exchange_data(url_data)
    df = pd.DataFrame(raw_data)

    bucket_ranges = [100, 500]
    bucketed_df = bucket_exchange_data(
        clean_exchange_data(df), "tradingPairs", bucket_ranges
    )

    grouped_df = bucketed_df.groupby("bucket").size().reset_index(name="count")
    return grouped_df


def run_exchange_data_pipeline():
    url = "https://api.coincap.io/v2/exchanges"
    print(transform_exchange_data(extract_coincap_api(url)))


if __name__ == "__main__":
    run_exchange_data_pipeline()
