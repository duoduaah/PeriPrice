{{ config(materialized='table') }}

SELECT
  s.store_nbr,
  s.city,
  s.state,
  s.type,
  s.cluster
FROM {{ ref('stg_stores') }} AS s
