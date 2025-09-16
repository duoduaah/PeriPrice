-- =========================================
-- 2) Enrich with lags/rolling + promo recency
--    and filter to rows with full 28d history
-- =========================================
CREATE OR REPLACE TABLE `@PROJECT.@ML_DATASET.features_enriched` AS
WITH base AS (
  SELECT * FROM `@PROJECT.@ML_DATASET.features_clean`
),

lags_rolls AS (
  SELECT
    b.*,

    -- -------- LAGS (no leakage: all are <= t-1)
    LAG(log_sales,  1)  OVER (PARTITION BY store_nbr, item_nbr ORDER BY date) AS lag1_log_sales,
    LAG(log_sales,  7)  OVER (PARTITION BY store_nbr, item_nbr ORDER BY date) AS lag7_log_sales,
    LAG(log_sales, 14)  OVER (PARTITION BY store_nbr, item_nbr ORDER BY date) AS lag14_log_sales,
    LAG(log_sales, 28)  OVER (PARTITION BY store_nbr, item_nbr ORDER BY date) AS lag28_log_sales,

    -- -------- ROLLING MEANS (ends at 1 PRECEDING to avoid leakage)
    AVG(log_sales) OVER (
      PARTITION BY store_nbr, item_nbr
      ORDER BY date
      ROWS BETWEEN 7  PRECEDING AND 1 PRECEDING
    ) AS rm7_log_sales,

    AVG(log_sales) OVER (
      PARTITION BY store_nbr, item_nbr
      ORDER BY date
      ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING
    ) AS rm28_log_sales,

    -- -------- PROMO RECENCY (any promo in last 7d, excluding today)
    MAX(CASE WHEN discount_pct > 0 THEN 1 ELSE 0 END) OVER (
      PARTITION BY store_nbr, item_nbr
      ORDER BY date
      ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
    ) AS promo_in_last_7d

  FROM base b
),

tag_history AS (
  -- Row index per (store,item) to guarantee 28d of history present
  SELECT
    lr.*,
    ROW_NUMBER() OVER (PARTITION BY store_nbr, item_nbr ORDER BY date) AS rn_item_store
  FROM lags_rolls lr
),

final AS (
  -- Filter rows lacking full windows so BQML doesn't drop NULL rows
  SELECT
    date, store_nbr, item_nbr, class, cluster, family,
    unit_sales, log_sales,
    base_price, discount_pct, effective_price, time_to_expiry,
    dow, month, year,
    lag1_log_sales, lag7_log_sales, lag14_log_sales, lag28_log_sales,
    rm7_log_sales, rm28_log_sales,
    promo_in_last_7d
  FROM tag_history
  WHERE rn_item_store > 28
)
SELECT * FROM final;
