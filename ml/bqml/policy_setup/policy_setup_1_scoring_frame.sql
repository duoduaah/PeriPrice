CREATE OR REPLACE TABLE `@PROJECT.dynamic_pricing_ml.scoring_frame_test`
PARTITION BY date AS
SELECT
  date, store_nbr, item_nbr,
  base_price, time_to_expiry,
  lag1_log_sales, lag7_log_sales, lag14_log_sales, lag28_log_sales,
  rm7_log_sales, rm28_log_sales, promo_in_last_7d,
  dow, month, year,
  family, class, store_nbr AS store_nbr_cat, cluster,
  discount_pct    AS baseline_discount_pct,
  effective_price AS baseline_effective_price,
  unit_sales      AS observed_unit_sales
FROM `@PROJECT.dynamic_pricing_ml.features_split`
WHERE split = 'test' AND time_to_expiry <= 2
