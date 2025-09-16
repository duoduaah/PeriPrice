{{ config(materialized='table') }}

-- considering national holidays only as regional/local holidays
-- can't be safely mapped yet (no idea which city or state corresponds to which locale)
-- Rules:
-- - 'Holiday' + transferred=TRUE  -> NOT a holiday on that date (moved elsewhere)
-- - 'Transfer'                    -> YES (celebrated day)
-- - 'Bridge'                      -> YES (extra day off)
-- - 'Additional'                  -> YES (extra day off)
-- - 'Work Day'                    -> NO  (make-up day)
-- - 'Event'                       -> NO  (not a public holiday)

WITH base AS (
  SELECT
    date,
    type,
    locale,
    transferred
  FROM {{ ref('stg_holidays') }}
),

tagged AS (
  SELECT
    date,
    locale,
    CASE
      WHEN type = 'Work Day' THEN 0
      WHEN type = 'Holiday' AND transferred IS TRUE THEN 0
      WHEN type IN ('Transfer','Bridge','Additional') THEN 1
      WHEN type = 'Holiday' AND (transferred IS FALSE OR transferred IS NULL) THEN 1
      ELSE 0  -- Event/other -> not a holiday effect
    END AS holiday_effect
  FROM base
)

SELECT
  date,
  CAST(
    MAX(CASE WHEN locale = 'National' AND holiday_effect = 1 THEN 1 ELSE 0 END)
    AS BOOL
  ) AS is_nat_holiday
FROM tagged
GROUP BY date
