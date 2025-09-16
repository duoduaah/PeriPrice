-- models/intermediate/int_base_price.sql
-- Goal: one stable base_price per item.
-- Logic:
--   • Each family gets a realistic price band [p_min, p_max]
--   • Each item_nbr gets a tiny deterministic jitter so items in the same family aren’t identical
--   • We DO NOT use `class` for price (class IDs are not ordinal)

WITH family_price_bands AS (
  SELECT 'BREAD_BAKERY'     AS family, 2.40 AS p_min, 4.00 AS p_max UNION ALL
  SELECT 'DAIRY'            AS family, 4.00,          7.00 UNION ALL
  SELECT 'DELI'             AS family, 5.00,         12.00 UNION ALL
  SELECT 'EGGS'             AS family, 3.00,          5.50 UNION ALL
  SELECT 'MEATS'            AS family, 10.00,        20.00 UNION ALL
  SELECT 'POULTRY'          AS family, 8.00,         15.00 UNION ALL
  SELECT 'PREPARED_FOODS'   AS family, 6.00,         15.00 UNION ALL
  SELECT 'PRODUCE'          AS family, 2.50,          7.00 UNION ALL
  SELECT 'SEAFOOD'          AS family, 15.00,        25.00
),

items AS (
  SELECT DISTINCT
    item_nbr,
    UPPER(family) AS family
  FROM {{ ref('stg_items') }}
),

joined AS (
  SELECT
    i.item_nbr,
    i.family,
    b.p_min,
    b.p_max,
    -- Deterministic pseudo-random in [0,1) per item for jitter
    ABS(MOD(FARM_FINGERPRINT(CAST(i.item_nbr AS STRING)), 1000000)) / 1000000.0 AS u_item
  FROM items i
  LEFT JOIN family_price_bands b USING (family)
),

priced AS (
  SELECT
    item_nbr,
    family,
    p_min,
    p_max,
    -- Band position centered at 0.5 with ±5% jitter from u_item
    0.50 + (u_item - 0.5) * 0.10 AS band_pos
  FROM joined
)

SELECT
  item_nbr,
  family,
  -- Final base price in family band with tiny deterministic variation
  ROUND(p_min + band_pos * (p_max - p_min), 2) AS base_price
FROM priced
