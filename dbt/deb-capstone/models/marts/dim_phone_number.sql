SELECT
    {{ dbt_utils.generate_surrogate_key(['review_logs.log_id']) }} AS id_dim_phone_number,
    review_logs.phone_number,
    movie_reviews.user_id AS user_id
FROM {{ ref('stg_review_logs') }} AS review_logs
JOIN {{ ref('stg_classified_movie_review') }} AS movie_reviews
ON review_logs.log_id = movie_reviews.user_id