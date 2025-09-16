-- =========================================
-- 3) Deterministic time-based split (based on date range)
--    Train: < 2017-07-18
--    Valid: 2017-07-18 .. 2017-07-31
--    Test : 2017-08-01 .. 2017-08-15
-- =========================================
CREATE OR REPLACE TABLE `@PROJECT.@ML_DATASET.features_split` AS
WITH bounds AS (
  SELECT
    DATE('2017-08-15')                            AS max_date,
    DATE_SUB(DATE('2017-08-15'), INTERVAL 28 DAY) AS valid_start,  -- 2017-07-18
    DATE_SUB(DATE('2017-08-15'), INTERVAL 14 DAY) AS test_start    -- 2017-08-01
),
labeled AS (
  SELECT
    fe.*,
    CASE
      WHEN fe.date < (SELECT valid_start FROM bounds) THEN 'train'
      WHEN fe.date < (SELECT test_start  FROM bounds) THEN 'valid'
      ELSE 'test'
    END AS split
  FROM `@PROJECT.@ML_DATASET.features_enriched` fe
)
SELECT * FROM labeled;
