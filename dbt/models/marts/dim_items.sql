/* {{ config(materialized='table') }}

SELECT
  i.item_nbr                              AS item_nbr,
  i.family,
  i.class,
  COALESCE(i.perishable_bool, FALSE)      AS perishable_bool
FROM {{ ref('int_items') }} AS i */

{{ config(materialized='table') }}

SELECT
  i.item_nbr,
  i.family,
  i.class,
  COALESCE(i.perishable_bool, FALSE) AS   perishable_bool,
  p.base_price
FROM {{ ref('int_items') }} i
LEFT JOIN {{ ref('int_base_price') }} p
  USING (item_nbr, family)
