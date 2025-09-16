{{ config(materialized='table') }}

SELECT
  date,
  is_nat_holiday
FROM {{ ref('int_holidays') }}
