{{ config(
  materialized='table',
  partition_by={'field':'date','data_type':'date'},
  cluster_by=['store_nbr','item_nbr']
) }}

-- Adds:
--   • shelf_life_days by family (Ontario-credible defaults)
--   • days_since_stock via deterministic hash (no arrival data available)
--   • time_to_expiry (clamped to 0)
--   • discount ladder by time_to_expiry
--   • effective_price = base_price * (1 - discount_pct)

WITH base AS (
  -- Cleaned sales (negatives->0, cap 200, bool promo, log_sales) already done upstream
  SELECT
    date,
    store_nbr,
    item_nbr,
    onpromotion,
    unit_sales,
    log_sales,
    EXTRACT(DAYOFWEEK FROM date) AS dow,
    EXTRACT(MONTH     FROM date) AS month,
    EXTRACT(YEAR      FROM date) AS year
  FROM {{ ref('int_sales') }}
),

with_items AS (
  SELECT
    b.*,
    di.family,
    di.class,
    di.perishable_bool,
    di.base_price
  FROM base b
  LEFT JOIN {{ ref('dim_items') }} di
    ON b.item_nbr = di.item_nbr
),

with_stores AS (
  SELECT
    w.*,
    ds.city,
    ds.state,
    ds.type,
    ds.cluster
  FROM with_items w
  LEFT JOIN {{ ref('dim_stores') }} ds
    ON w.store_nbr = ds.store_nbr
),

with_oil AS (
  SELECT
    w.*,
    doil.dcoil        AS oil_price,
    doil.dcoil_ma7    AS oil_price_ma7,
    doil.dcoil_ma30   AS oil_price_ma30,
    doil.pct_change_1d,
    doil.pct_change_7d
  FROM with_stores w
  LEFT JOIN {{ ref('dim_oil_prices') }} doil
    ON w.date = doil.date
),

with_shelf_life AS (
  -- Pragmatic defaults for Ontario retail perishables:
  --   BREAD_BAKERY: 3 (fresh bread/pastries)
  --   DAIRY:        10 
  --   DELI:         5  
  --   EGGS:         21 
  --   MEATS:        7
  --   POULTRY:      5
  --   PREPARED_FOODS: 3 
  --   PRODUCE:      5
  --   SEAFOOD:      5
  SELECT
    o.*,
    CASE o.family
      WHEN 'BREAD_BAKERY'   THEN 3
      WHEN 'DAIRY'          THEN 10
      WHEN 'DELI'           THEN 5
      WHEN 'EGGS'           THEN 21
      WHEN 'MEATS'          THEN 7
      WHEN 'POULTRY'        THEN 5
      WHEN 'PREPARED_FOODS' THEN 3
      WHEN 'PRODUCE'        THEN 5
      WHEN 'SEAFOOD'        THEN 5
      ELSE 14  -- fallback for any unexpected family
    END AS shelf_life_days
  FROM with_oil o
),

with_stock_age AS (
  -- Simulate stocking age deterministically so it's stable:
  --   days_since_stock ∈ [0, shelf_life_days], derived from a hash of (store_nbr, item_nbr, date)
  SELECT
    w.*,
    CAST(
      MOD(
        ABS(
          FARM_FINGERPRINT(
            CONCAT(CAST(w.store_nbr AS STRING), '-', CAST(w.item_nbr AS STRING), '-', CAST(w.date AS STRING))
          )
        ),
        w.shelf_life_days + 1
      ) AS INT64
    ) AS days_since_stock
  FROM with_shelf_life w
),

with_expiry AS (
  -- Expiry pressure feature (never negative)
  SELECT
    *,
    GREATEST(shelf_life_days - days_since_stock, 0) AS time_to_expiry
  FROM with_stock_age
),

with_discount AS (
  -- Markdown policy ladder tied to time_to_expiry which mirrors retail clearance behavior (tunable though).
  SELECT
    we.*,
    CASE
      WHEN time_to_expiry >= 6 THEN 0.00
      WHEN time_to_expiry BETWEEN 4 AND 5 THEN 0.10
      WHEN time_to_expiry = 3 THEN 0.20
      WHEN time_to_expiry = 2 THEN 0.30
      WHEN time_to_expiry = 1 THEN 0.40
      ELSE 0.50  -- time_to_expiry = 0 → clearance
    END AS discount_pct
  FROM with_expiry we
),

with_effective_price AS (
  -- What the shopper pays today at the ladder discount
  SELECT
    *,
    ROUND(base_price * (1 - discount_pct), 2) AS effective_price
  FROM with_discount
),

lags AS (
  -- Leakage-safe rolling features (exclude current day with "1 PRECEDING")
  SELECT
    store_nbr,
    item_nbr,
    date,
    AVG(unit_sales) OVER (
      PARTITION BY store_nbr, item_nbr
      ORDER BY date
      ROWS BETWEEN 7  PRECEDING AND 1 PRECEDING
    ) AS avg_units_7d,
    AVG(unit_sales) OVER (
      PARTITION BY store_nbr, item_nbr
      ORDER BY date
      ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
    ) AS avg_units_14d
  FROM base
)

-- Final projection: all features then perishable filtering
SELECT
  f.date,
  f.store_nbr,
  f.item_nbr,
  f.family,
  f.class,
  f.perishable_bool,
  f.onpromotion,
  f.unit_sales,
  f.log_sales,

  -- Price & expiry signals
  f.base_price,
  f.discount_pct,
  f.effective_price,
  f.shelf_life_days,
  f.days_since_stock,
  f.time_to_expiry,

  -- Temporal & rolling features
  f.dow,
  f.month,
  f.year,
  l.avg_units_7d,
  l.avg_units_14d,

  -- Optional store & macro signals
  f.city,
  f.state,
  f.type,
  f.cluster,
  f.oil_price,
  f.oil_price_ma7,
  f.oil_price_ma30,
  f.pct_change_1d,
  f.pct_change_7d

FROM with_effective_price f
LEFT JOIN lags l
  ON f.store_nbr = l.store_nbr
 AND f.item_nbr  = l.item_nbr
 AND f.date      = l.date
WHERE f.perishable_bool = TRUE
