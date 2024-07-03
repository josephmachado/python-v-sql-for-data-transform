from datetime import datetime
from decimal import Decimal

import pytest
import pytz

# Assume these functions are defined in the module where clean_exchange_data is located
from src.native_python import clean_exchange_data, get_utc_from_unix_time


@pytest.fixture
def sample_data():
    return [
        {
            "percentTotalVolume": "0.5",
            "updated": 1622471123,
            "volumeUsd": "1500000",
            "tradingPairs": "10",
        },
        {
            "percentTotalVolume": "1.2",
            "updated": 1622471124,
            "volumeUsd": None,
            "tradingPairs": "15",
        },
        {
            "percentTotalVolume": "0.3",
            "updated": 1622471125,
            "volumeUsd": "500000",
            "tradingPairs": "8",
        },
    ]


def test_clean_exchange_data(sample_data):
    expected_data = [
        {
            "percentTotalVolume": Decimal("0.5"),
            "updated": get_utc_from_unix_time(1622471123),
            "volumeUsd": Decimal("1500000"),
            "tradingPairs": 10,
        },
        {
            "percentTotalVolume": Decimal("0.3"),
            "updated": get_utc_from_unix_time(1622471125),
            "volumeUsd": Decimal("500000"),
            "tradingPairs": 8,
        },
    ]

    result = clean_exchange_data(sample_data)

    assert len(result) == len(expected_data)
    for entry, expected_entry in zip(result, expected_data):
        assert entry["percentTotalVolume"] == expected_entry["percentTotalVolume"]
        assert entry["updated"] == expected_entry["updated"]
        assert entry["volumeUsd"] == expected_entry["volumeUsd"]
        assert entry["tradingPairs"] == expected_entry["tradingPairs"]
