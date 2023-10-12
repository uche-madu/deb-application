WITH raw_user_purchase AS (
    SELECT * FROM {{ source('movie_analytics', 'user_purchase') }}
),

processed_user_purchase AS (
    SELECT
        invoice_number,
        stock_code,
        detail,
        cast(quantity as INT64),
        cast(invoice_date as TIMESTAMP),
        cast(unit_price as NUMERIC),
        cast(customer_id as INT),
        country
    FROM
        raw_user_purchase
    WHERE
        invoice_number IS NOT NULL
        AND stock_code IS NOT NULL
        AND quantity IS NOT NULL
        AND invoice_date IS NOT NULL
        AND unit_price IS NOT NULL
        AND customer_id IS NOT NULL
        AND country IS NOT NULL
)

SELECT * FROM processed_user_purchase

    









