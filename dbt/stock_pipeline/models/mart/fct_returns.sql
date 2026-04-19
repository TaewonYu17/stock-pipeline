-- models/mart/fct_returns.sql
-- Computed returns at multiple horizons.
-- LAG() looks at the previous row's close price for the same ticker.

WITH prices AS (
    SELECT * FROM {{ ref('fct_daily_prices') }}
),

returns AS (
    SELECT
        ticker,
        date,
        close,

        -- Daily log return: ln(today / yesterday)
        -- Log returns are used instead of simple returns because
        -- they're additive over time and handle compounding correctly
        LN(close / NULLIF(LAG(close, 1) OVER (
            PARTITION BY ticker ORDER BY date
        ), 0)) AS return_1d,

        -- Weekly return (5 trading days)
        LN(close / NULLIF(LAG(close, 5) OVER (
            PARTITION BY ticker ORDER BY date
        ), 0)) AS return_5d,

        -- Monthly return (21 trading days)
        LN(close / NULLIF(LAG(close, 21) OVER (
            PARTITION BY ticker ORDER BY date
        ), 0)) AS return_21d

    FROM prices
)

SELECT * FROM returns
WHERE return_1d IS NOT NULL  -- drop the first row per ticker (no previous close)