SELECT *
FROM {{ ref('int_sales_clean') }}
WHERE unit_sales < 0
