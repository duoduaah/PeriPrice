SELECT
  CAST(date AS DATE)          AS date,
  CAST(type AS STRING)        AS type,        -- 'Holiday','Transfer','Bridge','Work Day','Additional','Event'
  CAST(locale AS STRING)      AS locale,      -- 'National','Regional','Local'
  CAST(locale_name AS STRING)  AS local_name,
  CAST(description AS STRING) AS description,
  CASE
    WHEN LOWER(CAST(transferred AS STRING)) IN ('true','1','yes')  THEN TRUE
    WHEN LOWER(CAST(transferred AS STRING)) IN ('false','0','no')  THEN FALSE
    ELSE NULL
  END AS transferred
FROM {{ source('raw_native','holidays') }}
