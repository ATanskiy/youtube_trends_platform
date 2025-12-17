

WITH src AS (
    SELECT
        id          AS language_id,
        name        AS language_name,
        created_at
    FROM bronze.languages
)

SELECT *
FROM src