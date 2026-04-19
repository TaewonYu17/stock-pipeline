WITH source AS (
    SELECT * FROM STOCK_DB.RAW.stock_prices
),

cleaned AS (
    SELECT
        UPPER(TRIM(ticker))     AS ticker,
        CAST(date AS DATE)      AS date,
        CAST(open AS FLOAT)     AS open,
        CAST(high AS FLOAT)     AS high,
        CAST(low AS FLOAT)      AS low,
        CAST(close AS FLOAT)    AS close,
        CAST(volume AS BIGINT)  AS volume
    FROM source
    WHERE
        ticker IS NOT NULL
        AND date IS NOT NULL
        AND close IS NOT NULL
        AND close > 0           -- a close price of 0 is bad data
        AND volume >= 0
)

SELECT * FROM cleaned