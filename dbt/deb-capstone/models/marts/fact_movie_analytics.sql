WITH 
user_purchase AS(
    SELECT * FROM {{ ref('stg_user_purchase') }}
),

dim_devices AS (
    SELECT * FROM {{ ref('dim_devices') }}
),

dim_location AS (
    SELECT * FROM {{ ref('dim_location') }}
),

dim_os AS (
    SELECT * FROM {{ ref('dim_os') }}
),

movie_reviews AS (
    SELECT * FROM {{ ref('stg_classified_movie_review') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['user_purchase.customer_id']) }} AS fact_key,
        user_purchase.customer_id AS customer_id,
        dim_devices.id_dim_devices,
        dim_location.id_dim_location,
        dim_os.id_dim_os,
        SUM(user_purchase.quantity * user_purchase.unit_price) AS amount_spent,
        SUM(movie_reviews.positive_review) AS review_score,
        COUNT(movie_reviews.review_id) AS review_count,
        movie_reviews.insert_date
    FROM
        user_purchase
    JOIN reviews 
    ON user_purchase.customer_id = reviews.user_id
    
    LEFT JOIN dim_devices
    ON movie_reviews.user_id = dim_devices.log_id

    LEFT JOIN dim_location
    ON movie_reviews.user_id = dim_location.log_id

    LEFT JOIN dim_os
    ON movie_reviews.user_id = dim_os.log_id

    GROUP BY 
        fact_key,
        customer_id,
        id_dim_devices,
        id_dim_location,
        id_dim_os,
        insert_date
    
    ORDER BY
        customer_id
)

SELECT * FROM final;