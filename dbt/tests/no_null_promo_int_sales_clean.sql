SELECT *
FROM {{ ref('int_sales_clean') }}
WHERE onpromotion IS NULL
