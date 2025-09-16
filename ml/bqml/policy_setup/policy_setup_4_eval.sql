-- CREATE OR REPLACE TABLE `psyched-circuit-470902-r2.dynamic_pricing_ml.policy_eval_test`
CREATE OR REPLACE TABLE `@PROJECT.dynamic_pricing_ml.lgb_policy_eval_test`
PARTITION BY date AS
SELECT * FROM UNNEST(ARRAY<STRUCT<
  date DATE,
  store_nbr STRING,
  item_nbr STRING,
  baseline_discount_pct FLOAT64,
  baseline_effective_price FLOAT64,
  pred_units_baseline FLOAT64,
  baseline_revenue FLOAT64,
  policy_discount_pct FLOAT64,
  policy_effective_price FLOAT64,
  pred_units_policy FLOAT64,
  policy_revenue FLOAT64
>>[])
