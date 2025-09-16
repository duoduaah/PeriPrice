-- Overall KPIs (single row)
CREATE OR REPLACE TABLE `@PROJECT.dynamic_pricing_ml.lgb_policy_kpis_test` AS
SELECT
  COUNT(*) AS n_rows,
  SUM(pe.baseline_revenue) AS baseline_rev,
  SUM(pe.policy_revenue)   AS policy_rev,
  SUM(pe.policy_revenue - pe.baseline_revenue) AS uplift_abs,
  SAFE_DIVIDE(SUM(pe.policy_revenue - pe.baseline_revenue),
              NULLIF(SUM(pe.baseline_revenue), 0)) AS uplift_pct,
  AVG(pe.baseline_discount_pct) AS avg_baseline_discount,
  AVG(pe.policy_discount_pct)   AS avg_policy_discount,
  -- need time_to_expiry from scoring_frame_test
  CORR(pe.policy_discount_pct, CAST(sf.time_to_expiry AS FLOAT64)) AS corr_discount_vs_expiry
FROM `@PROJECT.dynamic_pricing_ml.lgb_policy_eval_test` pe
JOIN `@PROJECT.dynamic_pricing_ml.scoring_frame_test` sf
  USING (date, store_nbr, item_nbr);

-- Daily KPIs 
CREATE OR REPLACE TABLE `@PROJECT.dynamic_pricing_ml.lgb_policy_kpis_by_date` AS
SELECT
  pe.date,
  SUM(pe.baseline_revenue) AS baseline_rev,
  SUM(pe.policy_revenue)   AS policy_rev,
  SAFE_DIVIDE(SUM(pe.policy_revenue) - SUM(pe.baseline_revenue),
              NULLIF(SUM(pe.baseline_revenue), 0)) AS uplift_pct
FROM `@PROJECT.dynamic_pricing_ml.lgb_policy_eval_test` pe
GROUP BY pe.date
ORDER BY pe.date;

-- By time-to-expiry bucket 
CREATE OR REPLACE TABLE `@PROJECT.dynamic_pricing_ml.lgb_policy_kpis_by_expiry` AS
WITH joined AS (
  SELECT pe.*, sf.time_to_expiry
  FROM `@PROJECT.dynamic_pricing_ml.lgb_policy_eval_test` pe
  JOIN `@PROJECT.dynamic_pricing_ml.scoring_frame_test` sf
    USING (date, store_nbr, item_nbr)
)
SELECT
  CASE
    WHEN time_to_expiry <= 1 THEN '0-1d'
    WHEN time_to_expiry <= 3 THEN '2-3d'
    WHEN time_to_expiry <= 5 THEN '4-5d'
    ELSE '6d+'
  END AS expiry_bucket,
  SUM(baseline_revenue) AS baseline_rev,
  SUM(policy_revenue)   AS policy_rev,
  SAFE_DIVIDE(SUM(policy_revenue) - SUM(baseline_revenue),
              NULLIF(SUM(baseline_revenue), 0)) AS uplift_pct,
  AVG(policy_discount_pct) AS avg_policy_discount
FROM joined
GROUP BY expiry_bucket
ORDER BY expiry_bucket;

-- Distribution of chosen discounts 
CREATE OR REPLACE TABLE `@PROJECT.dynamic_pricing_ml.lgb_policy_discount_dist` AS
SELECT
  ROUND(policy_discount_pct, 1) AS discount_bin,
  COUNT(*) AS n_rows
FROM `@PROJECT.dynamic_pricing_ml.lgb_policy_eval_test`
GROUP BY discount_bin
ORDER BY discount_bin;
