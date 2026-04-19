-- models/staging/stg_macro.sql
-- Clean macro indicator data.

WITH source AS (
    SELECT * FROM {{ source('raw', 'economic_indicators') }}
),

cleaned AS (
    SELECT
        UPPER(series_id)        AS series_id,
        LOWER(indicator)        AS indicator,
        CAST(date AS DATE)      AS date,
        CAST(value AS FLOAT)    AS value
    FROM source
    WHERE
        date IS NOT NULL
        AND value IS NOT NULL
)

SELECT * FROM cleaned