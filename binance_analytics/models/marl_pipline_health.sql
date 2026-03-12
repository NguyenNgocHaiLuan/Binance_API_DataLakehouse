{{config(materialized='table')}}

-- Dashboard 3: Pipeline Health
-- Monitoring data ingestion quality & freshness

WITH hourly_ingestion AS (
    SELECT
        DATE_TRUNC('hour', candle_start_time)   AS ingestion_hour,
        symbol,
        COUNT(*)                                AS candles_count,
        SUM(trade_count)                        AS total_trades,
        SUM(total_volume)                       AS total_volume,
        MAX(ingested_at)                        AS last_ingested_at,
        MIN(candle_start_time)                  AS earliest_candle,
        MAX(candle_start_time)                  AS latest_candle,
        -- Expected 60 candles/hour (1 per minute)
        ROUND(COUNT(*) / 60.0 * 100, 1)        AS completeness_pct,
        -- Kiểm tra gaps
        60 - COUNT(*)                           AS missing_candles
    FROM {{ source('warehouse', 'fact_market_candles') }}
    GROUP BY DATE_TRUNC('hour', candle_start_time), symbol
),

freshness AS (
    SELECT
        symbol,
        MAX(candle_start_time)                  AS latest_candle_time,
        MAX(ingested_at)                        AS latest_ingestion_time,
        EXTRACT(EPOCH FROM (NOW() - MAX(candle_start_time))) / 60
                                                AS minutes_since_last_candle,
        CASE
            WHEN EXTRACT(EPOCH FROM (NOW() - MAX(candle_start_time))) / 60 <= 5
                THEN 'Fresh ✅'
            WHEN EXTRACT(EPOCH FROM (NOW() - MAX(candle_start_time))) / 60 <= 30
                THEN 'Stale ⚠️'
            ELSE 'Dead ❌'
        END                                     AS freshness_status
    FROM {{ source('warehouse', 'fact_market_candles') }}
    GROUP BY symbol
),

daily_summary AS (
    SELECT
        DATE_TRUNC('day', candle_start_time)    AS date,
        COUNT(*)                                AS total_candles,
        COUNT(DISTINCT symbol)                  AS active_symbols,
        SUM(trade_count)                        AS total_trades,
        SUM(total_volume)                       AS total_volume,
        AVG(trade_count)                        AS avg_trades_per_candle,
        -- Overall completeness (24h * 60min * n_symbols expected)
        ROUND(
            COUNT(*) / NULLIF(
                COUNT(DISTINCT symbol) * 24.0 * 60, 0
            ) * 100, 1
        )                                       AS daily_completeness_pct
    FROM {{ source('warehouse', 'fact_market_candles') }}
    GROUP BY DATE_TRUNC('day', candle_start_time)
)

SELECT
    h.ingestion_hour,
    h.symbol,
    h.candles_count,
    h.total_trades,
    h.total_volume,
    h.last_ingested_at,
    h.earliest_candle,
    h.latest_candle,
    h.completeness_pct,
    h.missing_candles,
    -- Freshness info
    f.latest_candle_time,
    f.minutes_since_last_candle,
    f.freshness_status,
    -- Daily context
    d.total_candles         AS day_total_candles,
    d.active_symbols        AS day_active_symbols,
    d.daily_completeness_pct
FROM hourly_ingestion h
LEFT JOIN freshness f
    ON h.symbol = f.symbol
LEFT JOIN daily_summary d
    ON DATE_TRUNC('day', h.ingestion_hour) = d.date
ORDER BY h.ingestion_hour DESC, h.symbol