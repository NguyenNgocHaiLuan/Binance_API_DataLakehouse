{{config(materialized='table')}}

-- Dashboard 2: OHLC Summary + Buy/Sell Analysis
-- Nến 1 phút với phân tích volume mua/bán

WITH candles AS (
    SELECT
        candle_start_time,
        symbol,
        open_price,
        high_price,
        low_price,
        close_price,
        total_volume,
        buy_volume_taker,
        sell_volume_maker,
        trade_count,
        -- Phân loại nến tăng/giảm
        CASE
            WHEN close_price >= open_price THEN 'Bullish'
            ELSE 'Bearish'
        END                                             AS candle_type,
        -- Body size %
        ROUND(
            ABS(close_price - open_price) / NULLIF(open_price, 0) * 100, 4
        )                                               AS body_size_pct,
        -- Buy pressure
        ROUND(
            buy_volume_taker / NULLIF(total_volume, 0) * 100, 2
        )                                               AS buy_pressure_pct,
        -- Giờ trong ngày (để heatmap)
        EXTRACT(HOUR FROM candle_start_time)            AS hour_of_day,
        DATE_TRUNC('hour', candle_start_time)           AS candle_hour,
        DATE_TRUNC('day',  candle_start_time)           AS candle_date
    FROM {{ source('warehouse', 'fact_market_candles') }}
),

hourly_agg AS (
    SELECT
        candle_hour,
        symbol,
        -- OHLC theo giờ
        FIRST_VALUE(open_price)  OVER (
            PARTITION BY symbol, candle_hour ORDER BY candle_start_time
        )                                               AS hourly_open,
        MAX(high_price)  OVER (PARTITION BY symbol, candle_hour) AS hourly_high,
        MIN(low_price)   OVER (PARTITION BY symbol, candle_hour) AS hourly_low,
        LAST_VALUE(close_price)  OVER (
            PARTITION BY symbol, candle_hour
            ORDER BY candle_start_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )                                               AS hourly_close,
        SUM(total_volume)    OVER (PARTITION BY symbol, candle_hour) AS hourly_volume,
        SUM(buy_volume_taker) OVER (PARTITION BY symbol, candle_hour) AS hourly_buy_vol,
        SUM(sell_volume_maker) OVER (PARTITION BY symbol, candle_hour) AS hourly_sell_vol,
        SUM(trade_count)     OVER (PARTITION BY symbol, candle_hour) AS hourly_trades,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, candle_hour ORDER BY candle_start_time DESC
        ) AS rn
    FROM candles
)

SELECT
    c.candle_start_time,
    c.symbol,
    c.open_price,
    c.high_price,
    c.low_price,
    c.close_price,
    c.total_volume,
    c.buy_volume_taker,
    c.sell_volume_maker,
    c.trade_count,
    c.candle_type,
    c.body_size_pct,
    c.buy_pressure_pct,
    c.hour_of_day,
    c.candle_hour,
    c.candle_date,
    -- Hourly aggregates
    h.hourly_open,
    h.hourly_high,
    h.hourly_low,
    h.hourly_close,
    h.hourly_volume,
    h.hourly_buy_vol,
    h.hourly_sell_vol,
    h.hourly_trades,
    ROUND(h.hourly_buy_vol / NULLIF(h.hourly_volume, 0) * 100, 2) AS hourly_buy_pct
FROM candles c
LEFT JOIN hourly_agg h
    ON c.symbol = c.symbol
    AND c.candle_hour = h.candle_hour
    AND h.rn = 1