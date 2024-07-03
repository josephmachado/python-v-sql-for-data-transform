from datetime import datetime
from decimal import Decimal

import pandas as pd
import pytest
import pytz

# Import the function and any necessary utilities from the module where they are defined
from src.dataframe_python import clean_exchange_data, get_utc_from_unix_time


@pytest.fixture
def sample_dataframe():
    data = {
        "percentTotalVolume": ["0.5", "1.2", "0.3"],
        "updated": [1622471123, 1622471124, 1622471125],
        "volumeUsd": ["1500000", None, "500000"],
        "tradingPairs": [10, 15, 8],
    }
    return pd.DataFrame(data)


def test_clean_exchange_data(sample_dataframe):
    expected_data = {
        "percentTotalVolume": [Decimal("0.5"), Decimal("0.3")],
        "updated": [
            get_utc_from_unix_time(1622471123),
            get_utc_from_unix_time(1622471125),
        ],
        "volumeUsd": [Decimal("1500000"), Decimal("500000")],
        "tradingPairs": [10, 8],
    }
    expected_df = pd.DataFrame(expected_data)

    result_df = clean_exchange_data(sample_dataframe)

    print(result_df)
    pd.testing.assert_frame_equal(
        result_df.reset_index(drop=True), expected_df.reset_index(drop=True)
    )
