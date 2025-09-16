SELECT
  CAST(store_nbr AS INT64)   AS store_nbr,
  CAST(city AS STRING)       AS city,
  CAST(state AS STRING)      AS state,
  CAST(type AS STRING)       AS type,
  SAFE_CAST(cluster AS INT64) AS cluster
FROM {{ source('raw_native', 'stores') }}
