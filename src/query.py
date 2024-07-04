import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
query = f"""
WITH exchange_data AS (
    SELECT UNNEST(data) AS data
    FROM read_json('https://api.coincap.io/v2/exchanges')
),
raw_data AS (
    SELECT 
        regexp_replace(json_extract(data, '$.exchangeId')::VARCHAR, '"', '', 'g') AS id,
        regexp_replace(json_extract(data, '$.name')::VARCHAR, '"', '', 'g') AS name,
        json_extract(data, '$.rank')::INTEGER AS rank,
        json_extract(data, '$.percentTotalVolume')::DOUBLE AS percentTotalVolume,
        json_extract(data, '$.volumeUsd')::DOUBLE AS volumeUsd,
        json_extract(data, '$.tradingPairs')::INTEGER AS tradingPairs,
        json_extract(data, '$.socket')::BOOLEAN AS socket,
        regexp_replace(json_extract(data, '$.exchangeUrl')::VARCHAR, '"', '', 'g') AS nexchangeUrl,
        json_extract(data, '$.updated')::BIGINT AS updated
    FROM exchange_data
),
filtered_data AS (
    SELECT
        percentTotalVolume::DECIMAL AS percentTotalVolume,
        to_timestamp(updated / 1000) AS updated,
        volumeUsd::DECIMAL AS volumeUSD,
        tradingPairs,
        CASE
            WHEN tradingPairs < 100 THEN '0-100'
            WHEN tradingPairs < 500 THEN '100-500'
            ELSE '500+'
        END AS bucket
    FROM raw_data
    WHERE volumeUsd IS NOT NULL
)
SELECT
    bucket,
    COUNT(*) AS count
FROM
    filtered_data
GROUP BY
    bucket
ORDER BY bucket;INSTALL httpfs;
LOAD httpfs;

WITH exchange_data AS (
    SELECT UNNEST(data) AS data
    FROM read_json('https://api.coincap.io/v2/exchanges')
),
raw_data AS (
    SELECT 
        regexp_replace(json_extract(data, '$.exchangeId')::VARCHAR, '"', '', 'g') AS id,
        regexp_replace(json_extract(data, '$.name')::VARCHAR, '"', '', 'g') AS name,
        json_extract(data, '$.rank')::INTEGER AS rank,
        json_extract(data, '$.percentTotalVolume')::DOUBLE AS percentTotalVolume,
        json_extract(data, '$.volumeUsd')::DOUBLE AS volumeUsd,
        json_extract(data, '$.tradingPairs')::INTEGER AS tradingPairs,
        json_extract(data, '$.socket')::BOOLEAN AS socket,
        regexp_replace(json_extract(data, '$.exchangeUrl')::VARCHAR, '"', '', 'g') AS nexchangeUrl,
        json_extract(data, '$.updated')::BIGINT AS updated
    FROM exchange_data
),
filtered_data AS (
    SELECT
        percentTotalVolume::DECIMAL AS percentTotalVolume,
        to_timestamp(updated / 1000) AS updated,
        volumeUsd::DECIMAL AS volumeUSD,
        tradingPairs,
        CASE
            WHEN tradingPairs < 100 THEN '0-100'
            WHEN tradingPairs < 500 THEN '100-500'
            ELSE '500+'
        END AS bucket
    FROM raw_data
    WHERE volumeUsd IS NOT NULL
)
SELECT
    bucket,
    COUNT(*) AS count
FROM
    filtered_data
GROUP BY
    bucket
ORDER BY bucket;
"""

# Execute the query and fetch results
result = con.execute(query).fetchall()

# Print the results
for row in result:
    print(row)

# Close the connection
con.close()
