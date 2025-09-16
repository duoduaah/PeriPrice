{{ config(materialized='table') }}

-- Purpose:
--   1) Build a complete daily spine over the active modeling window
--      (from the *later* of first non-NULL oil date and min sales date,
--       to the max sales date).
--   2) Join raw oil prices and forward-fill gaps (no future peeking).
--   3) Produce smoothed and change-rate features (MA7/MA30, pct changes).
--
-- Rationale:
--   - Starting the spine at the first non-NULL oil date which avoids an initial NULL.
--   - Forward fill mirrors what would be known at prediction time.
--   - Moving averages reduce day-to-day noise; pct changes capture trend shifts.

-- Sales date bounds (cleaned sales)
WITH sales_bounds AS (
  SELECT
    MIN(date) AS min_sales_date,
    MAX(date) AS max_sales_date
  FROM {{ ref('int_sales') }}
),

-- First oil date with an actual price (ignore NULLs)
oil_start AS (
  SELECT
    MIN(date) AS min_oil_date
  FROM {{ ref('stg_oil_prices') }}
  WHERE dcoil_raw IS NOT NULL
),

-- Final bounds: start at the later of (min sales date, first non-NULL oil date)
bounds AS (
  SELECT
    GREATEST(s.min_sales_date, o.min_oil_date) AS start_date,
    s.max_sales_date                            AS end_date
  FROM sales_bounds s, oil_start o
),

-- Daily calendar spine for the modeling window
calendar AS (
  SELECT d AS date
  FROM bounds b, UNNEST(GENERATE_DATE_ARRAY(b.start_date, b.end_date)) AS d
),

-- Join typed oil prices to the calendar (may be NULL on some days)
joined AS (
  SELECT
    c.date,
    o.dcoil_raw
  FROM calendar c
  LEFT JOIN {{ ref('stg_oil_prices') }} o
    USING (date)
),

-- Forward-fill oil price without using future information
ffill AS (
  SELECT
    date,
    LAST_VALUE(dcoil_raw IGNORE NULLS) OVER (
      ORDER BY date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS dcoil
  FROM joined
)

-- Final feature set
SELECT
  date,
  dcoil,  

  -- 7-day moving average (short-term smoothing). NULL for first 6 days.
  AVG(dcoil) OVER (
    ORDER BY date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS dcoil_ma7,

  -- 30-day moving average (medium-term smoothing). NULL for first 29 days.
  AVG(dcoil) OVER (
    ORDER BY date
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS dcoil_ma30,

  -- Day-over-day percent change (uses only past values).
  SAFE_DIVIDE(
    dcoil - LAG(dcoil, 1) OVER (ORDER BY date),
    LAG(dcoil, 1) OVER (ORDER BY date)
  ) AS pct_change_1d,

  -- Week-over-week percent change (uses only past values).
  SAFE_DIVIDE(
    dcoil - LAG(dcoil, 7) OVER (ORDER BY date),
    LAG(dcoil, 7) OVER (ORDER BY date)
  ) AS pct_change_7d

FROM ffill 
