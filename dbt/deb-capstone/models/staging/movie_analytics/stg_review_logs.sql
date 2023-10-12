SELECT 
    *
FROM 
    {{ source('movie_analytics', 'review_log') }}
WHERE
    log_id IS NOT NULL

