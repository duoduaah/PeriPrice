CREATE OR REPLACE TABLE `@PROJECT.dynamic_pricing_ml.scoring_candidates_test`
PARTITION BY date AS
SELECT * FROM UNNEST(ARRAY<STRUCT<
  date DATE,
  store_nbr STRING,
  item_nbr STRING,
  cand_discount_pct FLOAT64,
  cand_effective_price FLOAT64,
  predicted_unit_sales FLOAT64,
  pred_units FLOAT64,
  base_price FLOAT64,
  time_to_expiry INT64,
  lag1_log_sales FLOAT64,
  lag7_log_sales FLOAT64,
  lag14_log_sales FLOAT64,
  lag28_log_sales FLOAT64,
  rm7_log_sales FLOAT64,
  rm28_log_sales FLOAT64,
  promo_in_last_7d INT64,
  dow INT64,
  month INT64,
  year INT64,
  family STRING,
  class STRING,
  store_nbr_cat STRING,
  cluster STRING
>>[])
