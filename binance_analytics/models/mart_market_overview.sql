{{config(materialized='table')}}

-- Dashboard 1: Market Overview
-- Tổng quan thị trường theo từng symbol trong 24h gần nhất

WITH latest_candle AS (
    SELECT
        symbol,
        close_price,
        candle_start_time,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY candle_start_time DESC) AS rn
    FROM {{ source('warehouse', 'fact_market_candles') }}
),

first_candle_24h AS (
    SELECT
        symbol,
        open_price,
        candle_start_time,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY candle_start_time ASC) AS rn
    FROM {{ source('warehouse', 'fact_market_candles') }}
    WHERE candle_start_time >= NOW() - INTERVAL '24 hours'
),

stats_24h AS (
    SELECT
        symbol,
        MAX(high_price)                         AS high_24h,
        MIN(low_price)                          AS low_24h,
        SUM(total_volume)                       AS volume_24h,
        SUM(buy_volume_taker)                   AS buy_volume_24h,
        SUM(sell_volume_maker)                  AS sell_volume_24h,
        SUM(trade_count)                        AS total_trades_24h
    FROM {{ source('warehouse', 'fact_market_candles') }}
    WHERE candle_start_time >= NOW() - INTERVAL '24 hours'
    GROUP BY symbol
)

SELECT
    s.symbol,
    l.close_price                               AS current_price,
    f.open_price                                AS open_price_24h,
    s.high_24h,
    s.low_24h,
    ROUND(
        (l.close_price - f.open_price) / NULLIF(f.open_price, 0) * 100, 2
    )                                           AS price_change_pct_24h,
    l.close_price - f.open_price               AS price_change_24h,
    s.volume_24h,
    s.buy_volume_24h,
    s.sell_volume_24h,
    ROUND(
        s.buy_volume_24h / NULLIF(s.volume_24h, 0) * 100, 2
    )                                           AS buy_ratio_pct,
    s.total_trades_24h,
    l.candle_start_time                         AS last_updated
FROM stats_24h s
LEFT JOIN latest_candle l ON s.symbol = l.symbol AND l.rn = 1
LEFT JOIN first_candle_24h f ON s.symbol = f.symbol AND f.rn = 1