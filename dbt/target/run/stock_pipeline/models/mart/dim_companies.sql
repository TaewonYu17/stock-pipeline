
  
    

        create or replace transient table STOCK_DB.MART.dim_companies
         as
        (-- models/mart/dim_companies.sql
-- Company dimension table. One row per ticker.

SELECT
    ticker,
    sector,
    industry,
    market_cap,
    pe_ratio,
    description
FROM STOCK_DB.STAGING.stg_companies
        );
      
  