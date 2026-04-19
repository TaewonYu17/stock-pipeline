-- models/features/price_features.sql
-- Rolling technical indicators per ticker per day.
-- This is the table Feast reads from as its offline store.

WITH prices AS (
    SELECT * FROM {{ ref('fct_daily_prices') }}
),

returns AS (
    SELECT * FROM {{ ref('fct_returns') }}
),

macro AS (
    -- Pivot macro indicators into columns for easy joining
    SELECT
        date,
        MAX(CASE WHEN indicator = 'vix'         THEN value END) AS vix_close,
        MAX(CASE WHEN indicator = 'treasury_10y' THEN value END) AS treasury_10y,
        MAX(CASE WHEN indicator = 'fed_rate'     THEN value END) AS fed_rate
    FROM {{ ref('stg_macro') }}
    GROUP BY date
),

-- Moving averages
moving_avgs AS (
    SELECT
        ticker,
        date,
        close,
        AVG(close) OVER (
            PARTITION BY ticker ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS ma_5,
        AVG(close) OVER (
            PARTITION BY ticker ORDER BY date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS ma_20,
        AVG(close) OVER (
            PARTITION BY ticker ORDER BY date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS ma_50,

        -- Volatility: standard deviation of close over 20 days
        STDDEV(close) OVER (
            PARTITION BY ticker ORDER BY date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS volatility_20d,

        -- Volume ratio: today's volume vs 20-day average
        volume / NULLIF(AVG(volume) OVER (
            PARTITION BY ticker ORDER BY date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ), 0) AS volume_ratio,

        -- 52-week high (252 trading days)
        MAX(close) OVER (
            PARTITION BY ticker ORDER BY date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS high_52w

    FROM prices
),

-- Bollinger Bands: mean ± 2 standard deviations
bollinger AS (
    SELECT
        ticker,
        date,
        ma_20 + (2 * volatility_20d) AS bb_upper,
        ma_20 - (2 * volatility_20d) AS bb_lower
    FROM moving_avgs
),

-- RSI (14-day Relative Strength Index)
-- Measures momentum: 0-30 oversold, 70-100 overbought
rsi_calc AS (
    SELECT
        ticker,
        date,
        close,
        close - LAG(close) OVER (PARTITION BY ticker ORDER BY date) AS price_change
    FROM prices
),

rsi_gains AS (
    SELECT
        ticker,
        date,
        AVG(CASE WHEN price_change > 0 THEN price_change ELSE 0 END) OVER (
            PARTITION BY ticker ORDER BY date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS avg_gain,
        AVG(CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END) OVER (
            PARTITION BY ticker ORDER BY date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS avg_loss
    FROM rsi_calc
),

rsi AS (
    SELECT
        ticker,
        date,
        CASE
            WHEN avg_loss = 0 THEN 100
            ELSE 100 - (100 / (1 + (avg_gain / NULLIF(avg_loss, 0))))
        END AS rsi_14
    FROM rsi_gains
),

-- MACD: 12-day EMA minus 26-day EMA
-- Snowflake doesn't have built-in EMA, so we approximate with simple moving averages
macd_calc AS (
    SELECT
        ticker,
        date,
        AVG(close) OVER (
            PARTITION BY ticker ORDER BY date
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS ema_12,
        AVG(close) OVER (
            PARTITION BY ticker ORDER BY date
            ROWS BETWEEN 25 PRECEDING AND CURRENT ROW
        ) AS ema_26
    FROM prices
),

macd AS (
    SELECT
        ticker,
        date,
        ema_12 - ema_26 AS macd,
        AVG(ema_12 - ema_26) OVER (
            PARTITION BY ticker ORDER BY date
            ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
        ) AS macd_signal
    FROM macd_calc
),

-- Sector return: average 1-week return across all tickers in the same sector
sector_returns AS (
    SELECT
        p.ticker,
        p.date,
        p.sector,
        AVG(r.return_5d) OVER (
            PARTITION BY p.sector, p.date
        ) AS sector_return_1w
    FROM prices p
    JOIN returns r ON p.ticker = r.ticker AND p.date = r.date
),

-- Final join: bring everything together
final AS (
    SELECT
        m.ticker,
        m.date,
        r.return_1d,
        m.ma_5,
        m.ma_20,
        m.ma_50,
        rsi.rsi_14,
        m.volatility_20d,
        m.volume_ratio,
        b.bb_upper,
        b.bb_lower,
        macd.macd,
        macd.macd_signal,
        m.close / NULLIF(m.high_52w, 0)  AS price_vs_52w_high,
        s.sector_return_1w,
        mac.vix_close
    FROM moving_avgs m
    LEFT JOIN returns       r    ON m.ticker = r.ticker     AND m.date = r.date
    LEFT JOIN rsi           rsi  ON m.ticker = rsi.ticker   AND m.date = rsi.date
    LEFT JOIN bollinger     b    ON m.ticker = b.ticker      AND m.date = b.date
    LEFT JOIN macd          macd ON m.ticker = macd.ticker  AND m.date = macd.date
    LEFT JOIN sector_returns s   ON m.ticker = s.ticker     AND m.date = s.date
    LEFT JOIN macro         mac  ON m.date   = mac.date
)

SELECT * FROM final
WHERE return_1d IS NOT NULL   -- need a label to train on