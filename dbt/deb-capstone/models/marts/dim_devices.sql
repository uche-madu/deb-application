SELECT
    {{ dbt_utils.generate_surrogate_key(['review_logs.log_id']) }} AS id_dim_devices,
    review_logs.device,
    movie_reviews.user_id AS user_id
FROM {{ ref('stg_review_logs') }} AS review_logs
JOIN {{ ref('stg_classified_movie_review') }} AS movie_reviews
ON review_logs.log_id = movie_reviews.user_id