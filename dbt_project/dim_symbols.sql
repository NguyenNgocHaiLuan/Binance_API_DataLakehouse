{{config(materialized='table')}}

WITH real_symbols AS (
    SELECT
        DISTINCT symbol,
        FROM {{source('warehouse_db', 'fact_market_candles')}}
),
metadata AS (
    SELECT
        *
    FROM {{ref('crypto_metadata')}}
)
SELECT
    a.symbol,
    UPPER(LEFT(r.symbol, LENGTH(a.symbol) - 4)) AS asset_code,
    'USDT' AS currency_code,
    COALESCE(b.name, UPPER(LEFT(a.symbol, LENGTH(a.symbol) - 4))) AS name,
    COALESCE(b.category, 'Others') as category,
    COALESCE(b.description, 'Unknown') as consensus,
    CURRENT_DATE as discovered_date
FROM real_symbols a
LEFT JOIN metadata b
ON a.symbol = b.symbol