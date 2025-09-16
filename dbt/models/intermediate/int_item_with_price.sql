{{ config(materialized='view') }}

SELECT
  i.item_nbr,
  i.family,
  i.class,
  i.perishable_bool,
  p.base_price
FROM {{ ref('int_items') }} i
LEFT JOIN {{ ref('int_base_price') }} p
  USING (item_nbr, family)
