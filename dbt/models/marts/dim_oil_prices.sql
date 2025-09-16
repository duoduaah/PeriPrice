{{ config(materialized='table') }}

SELECT
  date,
  dcoil,
  dcoil_ma7,
  dcoil_ma30,
  pct_change_1d,
  pct_change_7d
FROM {{ ref('int_oil_prices') }}
