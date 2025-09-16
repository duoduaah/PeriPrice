SELECT
  CAST(item_nbr AS INT64)        AS item_nbr,

  -- Clean family names:
  -- Step 1: cast to STRING
  -- Step 2: replace "/" with "_"
  -- Step 3: replace spaces with "_"
  REGEXP_REPLACE(
    REGEXP_REPLACE(CAST(family AS STRING), r'/', '_'),
    r' ', '_'
  )                              AS family,

  SAFE_CAST(class AS INT64)      AS class,
  SAFE_CAST(perishable AS INT64) AS perishable

FROM {{ source('raw_native','items') }}
