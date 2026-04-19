-- models/mart/fct_daily_prices.sql
-- Fact table: one row per ticker per trading day.
-- Joins prices with company sector for downstream context.

WITH prices AS (
    SELECT * FROM STOCK_DB.STAGING.stg_prices
),

companies AS (
    SELECT ticker, sector FROM STOCK_DB.STAGING.stg_companies
),

joined AS (
    SELECT
        p.ticker,
        p.date,
        p.open,
        p.high,
        p.low,
        p.close,
        p.volume,
        c.sector
    FROM prices p
    LEFT JOIN companies c ON p.ticker = c.ticker
)

SELECT * FROM joined