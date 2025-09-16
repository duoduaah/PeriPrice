SELECT
    CAST(date as DATE)          AS date,
    SAFE_CAST(dcoilwtico AS FLOAT64)    AS dcoil_raw
FROM {{ source('raw_native', 'oil_prices') }}