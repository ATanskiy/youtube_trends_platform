

WITH src AS (
    SELECT
        id          AS language_id,
        name        AS language_name,
        CAST(created_at AS TIMESTAMP) AS created_at
    FROM bronze.languages
)

SELECT *
FROM src