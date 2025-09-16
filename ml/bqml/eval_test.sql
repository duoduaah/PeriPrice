SELECT 'valid' AS eval_split, * FROM ML.EVALUATE(
  MODEL `@PROJECT.dynamic_pricing_ml.bqml_pricing_gbt`,
  (SELECT * FROM `@PROJECT.dynamic_pricing_ml.features_split` WHERE split='valid')
)
