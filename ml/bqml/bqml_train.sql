-- Train BQML baseline (Boosted Trees) on the TRAIN slice
CREATE OR REPLACE MODEL `@PROJECT.dynamic_pricing_ml.bqml_pricing_gbt`
OPTIONS (
  model_type = 'BOOSTED_TREE_REGRESSOR',
  input_label_cols = ['unit_sales'],
  max_iterations = 600,
  max_tree_depth = 8,
  subsample = 0.8,
  min_split_loss = 0.0,
  early_stop = TRUE,
  enable_global_explain = TRUE
) AS
SELECT
  -- Label
  unit_sales,

  -- Core price/expiry
  effective_price,
  discount_pct,
  time_to_expiry,
  base_price,

  -- Autoregressive signal (safe ≤ t-1)
  lag1_log_sales, lag7_log_sales, lag14_log_sales, lag28_log_sales,
  rm7_log_sales,  rm28_log_sales,
  promo_in_last_7d,

  -- Calendar
  dow, month, year,

  -- Categoricals (BQML handles STRING categoricals)
  family, class, store_nbr, cluster

FROM `@PROJECT.dynamic_pricing_ml.features_split`
WHERE split = 'train'
