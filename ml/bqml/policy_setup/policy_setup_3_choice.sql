CREATE OR REPLACE TABLE `@PROJECT.dynamic_pricing_ml.policy_choice_test`
PARTITION BY date AS
SELECT * FROM UNNEST(ARRAY<STRUCT<
  date DATE,
  store_nbr STRING,
  item_nbr STRING,
  policy_discount_pct FLOAT64,
  policy_effective_price FLOAT64,
  pred_units_policy FLOAT64,
  policy_revenue FLOAT64
>>[])

