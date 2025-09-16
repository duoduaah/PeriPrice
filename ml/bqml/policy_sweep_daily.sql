-- ==========================================
-- Dynamic Pricing Policy Sweep (daily + sharded)
-- ==========================================

-- Candidate discount grid
DECLARE discount_grid ARRAY<FLOAT64> DEFAULT [0.0, 0.1, 0.2, 0.3, 0.4, 0.5];

-- Test period 
DECLARE start_date DATE DEFAULT DATE('2017-08-01');
DECLARE end_date   DATE DEFAULT DATE('2017-08-15');

-- Sharding to keep each job small 
DECLARE NUM_SHARDS INT64 DEFAULT 16;

-- Loop vars
DECLARE d DATE DEFAULT start_date;
DECLARE shard INT64 DEFAULT 0;

WHILE d <= end_date DO
  -- reset shard each day
  SET shard = 0;

  WHILE shard < NUM_SHARDS DO

    -- -------------------------------------------------
    -- 0) Idempotent cleanup for this (day, shard) slice
    -- -------------------------------------------------
    DELETE FROM `@PROJECT.dynamic_pricing_ml.scoring_candidates_test`
    WHERE date = d
      AND MOD(ABS(FARM_FINGERPRINT(CONCAT(store_nbr, '|', item_nbr))), NUM_SHARDS) = shard;

    DELETE FROM `@PROJECT.dynamic_pricing_ml.policy_choice_test`
    WHERE date = d
      AND MOD(ABS(FARM_FINGERPRINT(CONCAT(store_nbr, '|', item_nbr))), NUM_SHARDS) = shard;

    DELETE FROM `@PROJECT.dynamic_pricing_ml.policy_eval_test`
    WHERE date = d
      AND MOD(ABS(FARM_FINGERPRINT(CONCAT(store_nbr, '|', item_nbr))), NUM_SHARDS) = shard;

    -- -------------------------------------------------
    -- 1) Build CANDIDATES for (day d, shard) as a TEMP table
    --    - Includes EXACT training feature names expected by the model
    --    - Plus keys and candidate controls we’ll want in outputs
    -- -------------------------------------------------
    CREATE OR REPLACE TEMP TABLE cand AS
    WITH base AS (
      SELECT
        sf.date,
        sf.store_nbr,                -- key (and model feature)
        sf.item_nbr,                 -- key
        sf.base_price,
        sf.time_to_expiry,
        sf.lag1_log_sales, sf.lag7_log_sales, sf.lag14_log_sales, sf.lag28_log_sales,
        sf.rm7_log_sales,  sf.rm28_log_sales,  sf.promo_in_last_7d,
        sf.dow, sf.month, sf.year,
        sf.family, sf.class, sf.cluster
      FROM `psyched-circuit-470902-r2.dynamic_pricing_ml.scoring_frame_test` sf
      WHERE sf.date = d
        AND MOD(ABS(FARM_FINGERPRINT(CONCAT(sf.store_nbr, '|', sf.item_nbr))), NUM_SHARDS) = shard
    )
    SELECT
      b.date,
      b.store_nbr,
      b.item_nbr,
      b.base_price,
      b.time_to_expiry,
      b.lag1_log_sales, b.lag7_log_sales, b.lag14_log_sales, b.lag28_log_sales,
      b.rm7_log_sales,  b.rm28_log_sales,  b.promo_in_last_7d,
      b.dow, b.month, b.year,
      b.family, b.class, b.cluster,

      -- candidate controls to keep
      dgrid AS cand_discount_pct,
      ROUND(b.base_price * (1 - dgrid), 2) AS cand_effective_price,

      -- training feature names expected by the model
      ROUND(b.base_price * (1 - dgrid), 2) AS effective_price,
      dgrid AS discount_pct
    FROM base b, UNNEST(discount_grid) AS dgrid;

    -- -------------------------------------------------
    -- 2) Score candidates and APPEND to scoring_candidates_test
    --    ML.PREDICT(TABLE cand) echoes inputs + adds predicted_unit_sales
    -- -------------------------------------------------
    INSERT INTO `@PROJECT.dynamic_pricing_ml.scoring_candidates_test`
    SELECT
      date,
      store_nbr,
      item_nbr,
      cand_discount_pct,
      cand_effective_price,
      predicted_unit_sales,                     -- from ML.PREDICT
      predicted_unit_sales AS pred_units,       -- alias for clarity
      base_price,
      time_to_expiry,
      lag1_log_sales, lag7_log_sales, lag14_log_sales, lag28_log_sales,
      rm7_log_sales, rm28_log_sales, promo_in_last_7d,
      dow, month, year,
      family, class,
      store_nbr AS store_nbr_cat,               -- fill both store_nbr & store_nbr_cat as per table schema
      cluster
    FROM ML.PREDICT(
      MODEL `@PROJECT.dynamic_pricing_ml.bqml_pricing_gbt`,
      TABLE cand
    );

    -- -------------------------------------------------
    -- 3) Pick the revenue-maximizing candidate for this (day, shard)
    -- -------------------------------------------------
    INSERT INTO `@PROJECT.dynamic_pricing_ml.policy_choice_test`
    SELECT
      date,
      store_nbr,
      item_nbr,
      cand_discount_pct     AS policy_discount_pct,
      cand_effective_price  AS policy_effective_price,
      pred_units            AS pred_units_policy,
      cand_effective_price * pred_units AS policy_revenue
    FROM (
      SELECT
        date, store_nbr, item_nbr,
        cand_discount_pct, cand_effective_price, pred_units,
        ROW_NUMBER() OVER (
          PARTITION BY date, store_nbr, item_nbr
          ORDER BY cand_effective_price * pred_units DESC
        ) AS rn
      FROM `@PROJECT.dynamic_pricing_ml.scoring_candidates_test`
      WHERE date = d
        AND MOD(ABS(FARM_FINGERPRINT(CONCAT(store_nbr, '|', item_nbr))), NUM_SHARDS) = shard
    )
    WHERE rn = 1;

    -- -------------------------------------------------
    -- 4) Baseline predictions (model-based) for this (day, shard) and APPEND eval
    --    - Build baseline inputs with baseline_* aliased to training feature names
    --    - Predict, selecting ONLY keys + prediction to avoid duplicate columns
    -- -------------------------------------------------
    CREATE OR REPLACE TEMP TABLE base_infer AS
    SELECT
      sf.date,
      sf.store_nbr,
      sf.item_nbr,
      -- alias baseline_* to the model’s feature names
      sf.baseline_effective_price AS effective_price,
      sf.baseline_discount_pct    AS discount_pct,
      -- remaining features used by the model
      sf.base_price,
      sf.time_to_expiry,
      sf.lag1_log_sales, sf.lag7_log_sales, sf.lag14_log_sales, sf.lag28_log_sales,
      sf.rm7_log_sales,  sf.rm28_log_sales, sf.promo_in_last_7d,
      sf.dow, sf.month, sf.year,
      sf.family, sf.class, sf.cluster,
      -- keep originals for outputs
      sf.baseline_effective_price,
      sf.baseline_discount_pct
    FROM `@PROJECT.dynamic_pricing_ml.scoring_frame_test` sf
    WHERE sf.date = d
      AND MOD(ABS(FARM_FINGERPRINT(CONCAT(sf.store_nbr, '|', sf.item_nbr))), NUM_SHARDS) = shard;

    INSERT INTO `@PROJECT.dynamic_pricing_ml.policy_eval_test`
    SELECT
      b.date, b.store_nbr, b.item_nbr,
      b.baseline_discount_pct,
      b.baseline_effective_price,
      pb.predicted_unit_sales AS pred_units_baseline,
      b.baseline_effective_price * pb.predicted_unit_sales AS baseline_revenue,
      pc.policy_discount_pct,
      pc.policy_effective_price,
      pc.pred_units_policy,
      pc.policy_revenue
    FROM base_infer b
    JOIN (
      SELECT
        date,
        store_nbr,
        item_nbr,
        predicted_unit_sales
      FROM ML.PREDICT(
        MODEL `@PROJECT.dynamic_pricing_ml.bqml_pricing_gbt`,
        TABLE base_infer
      )
    ) AS pb
      USING (date, store_nbr, item_nbr)
    JOIN `@PROJECT.dynamic_pricing_ml.policy_choice_test` pc
      USING (date, store_nbr, item_nbr);

    -- next shard
    SET shard = shard + 1;

  END WHILE;

  -- next day
  SET d = DATE_ADD(d, INTERVAL 1 DAY);

END WHILE;
