-- tests/assert_close_price_reasonable.sql
-- Fails if any close price is more than 10x the previous day's close.
-- Catches obvious bad data (stock splits not adjusted, API errors).
-- dbt "singular tests" fail if the query returns ANY rows.

SELECT
    ticker,
    date,
    close,
    LAG(close) OVER (PARTITION BY ticker ORDER BY date) AS prev_close,
    close / NULLIF(LAG(close) OVER (PARTITION BY ticker ORDER BY date), 0) AS ratio
FROM {{ ref('fct_daily_prices') }}
QUALIFY
    close / NULLIF(LAG(close) OVER (PARTITION BY ticker ORDER BY date), 0) > 10