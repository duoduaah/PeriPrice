-- Validation metrics
CREATE OR REPLACE TABLE `@PROJECT.dynamic_pricing_ml.bqml_eval_valid` AS
SELECT * FROM ML.EVALUATE(
  MODEL `@PROJECT.dynamic_pricing_ml.bqml_pricing_gbt`,
  (SELECT * FROM `@PROJECT.dynamic_pricing_ml.features_split` WHERE split = 'valid')
);

-- Test metrics
CREATE OR REPLACE TABLE `@PROJECT.dynamic_pricing_ml.bqml_eval_test` AS
SELECT * FROM ML.EVALUATE(
  MODEL `@PROJECT.dynamic_pricing_ml.bqml_pricing_gbt`,
  (SELECT * FROM `@PROJECT.dynamic_pricing_ml.features_split` WHERE split = 'test')
);

-- Feature importance
CREATE OR REPLACE TABLE `@PROJECT.dynamic_pricing_ml.bqml_feature_importance` AS
SELECT *
FROM ML.FEATURE_IMPORTANCE(MODEL `@PROJECT.dynamic_pricing_ml.bqml_pricing_gbt`);

