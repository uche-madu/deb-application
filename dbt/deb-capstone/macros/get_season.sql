{% macro get_season(date_column) %}

    CASE
        WHEN EXTRACT(MONTH FROM {{ date_column }}) BETWEEN 3 AND 5 THEN 'Spring'
        WHEN EXTRACT(MONTH FROM {{ date_column }}) BETWEEN 6 AND 8 THEN 'Summer'
        WHEN EXTRACT(MONTH FROM {{ date_column }}) BETWEEN 9 AND 11 THEN 'Fall'
        ELSE 'Winter'
    END

{% endmacro %}