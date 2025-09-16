SELECT
  item_nbr,
  family,
  class,
  CASE WHEN perishable = 1 THEN TRUE
       WHEN perishable = 0 THEN FALSE
       ELSE NULL
  END AS perishable_bool
FROM {{ ref('stg_items') }}
