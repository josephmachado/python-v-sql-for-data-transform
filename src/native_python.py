from datetime import datetime
from decimal import Decimal
from itertools import groupby
from operator import itemgetter
from pprint import pprint
from typing import Any, Dict, List, Optional

import pytz
import requests


def extract_coincap_api(url):
    """
    Function to pull data from url
    and return the data
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx and 5xx)
        data = response.json()
        return data
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred: {req_err}")


def flatten_exchange_data(
    data: Dict[str, List[Dict[str, str]]]
) -> List[Dict[str, str]]:
    return data.get("data", [])


def get_utc_from_unix_time(
    unix_ts: Optional[Any], second: int = 1000
) -> Optional[datetime]:
    return datetime.fromtimestamp(int(unix_ts) / second) if unix_ts else None


def clean_exchange_data(data):
    # use Decimal for percentTotalVolume, volumeUsd
    # convert updated to datetime
    # if null volumeUsd drop rows
    processed_data = []
    for entry in data:
        if entry["volumeUsd"] is not None:
            entry["percentTotalVolume"] = Decimal(entry["percentTotalVolume"])
            entry["updated"] = get_utc_from_unix_time(entry["updated"])
            entry["volumeUsd"] = Decimal(entry["volumeUsd"])
            entry["tradingPairs"] = int(entry["tradingPairs"])
            processed_data.append(entry)
    return processed_data


def determine_bucket(value, ranges):
    for i, upper_limit in enumerate(ranges):
        if value < upper_limit:
            return f"{ranges[i-1] if i > 0 else 0}-{upper_limit}"
    return f"{ranges[-1]}+"


def bucket_data(data, column_to_bucket_by, bucket_ranges):
    processed_data = []
    for entry in data:
        if entry[column_to_bucket_by] is not None:
            bucket = determine_bucket(entry[column_to_bucket_by], bucket_ranges)
            entry[f"{column_to_bucket_by}_bucket"] = bucket
            processed_data.append(entry)
    return processed_data


def aggregate_exchange_data(data, agg_col):
    # Bucket by trading pair ranges 0-100, 100-500, 500+
    # aggregate by trading pair range and do a count

    # Sort data by bucket to prepare for grouping
    data.sort(key=itemgetter(agg_col))

    # Group by bucket and count occurrences
    return {
        agg_col: len(list(group))
        for agg_col, group in groupby(data, key=itemgetter(agg_col))
    }


def transform_exchange_data(data):
    transformed_data = aggregate_exchange_data(
        bucket_data(
            clean_exchange_data(flatten_exchange_data(data)), "tradingPairs", [100, 500]
        ),
        "tradingPairs_bucket",
    )
    return transformed_data


def run_exchange_data_pipeline():
    url = "https://api.coincap.io/v2/exchanges"
    pprint(transform_exchange_data(extract_coincap_api(url)))


if __name__ == "__main__":
    run_exchange_data_pipeline()
