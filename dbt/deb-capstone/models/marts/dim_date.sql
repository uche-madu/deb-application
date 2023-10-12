SELECT 
    {{ dbt_utils.generate_surrogate_key(['review_logs.log_id']) }} as id_dim_date,
    review_logs.log_date,
    EXTRACT(DAY FROM review_logs.log_date) AS day,
    EXTRACT(MONTH FROM review_logs.log_date) AS month,
    EXTRACT(YEAR FROM review_logs.log_date) AS year,
    {{ get_season('review_logs.log_date')}} AS season,
    movie_reviews.user_id AS user_id
FROM {{ ref('stg_review_logs') }} AS review_logs
JOIN {{ ref('stg_classified_movie_review') }} AS movie_reviews
ON review_logs.log_id = movie_reviews.user_id