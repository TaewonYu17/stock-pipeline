-- models/staging/stg_companies.sql
-- Normalize company metadata.

WITH source AS (
    SELECT * FROM {{ source('raw', 'company_info') }}
),

cleaned AS (
    SELECT
        UPPER(TRIM(ticker))         AS ticker,
        INITCAP(sector)             AS sector,
        INITCAP(industry)           AS industry,
        TRY_CAST(market_cap AS BIGINT)  AS market_cap,
        TRY_CAST(pe_ratio AS FLOAT)     AS pe_ratio,
        description
    FROM source
    WHERE ticker IS NOT NULL
)

SELECT * FROM cleaned