{{ config(materialized='view') }}

WITH ranked AS (
    SELECT
        v:id::INTEGER           AS customer_id,
        v:first_name::STRING    AS first_name,
        v:last_name::STRING     AS last_name,
        v:email::STRING         AS email,
        v:created_at::TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP       AS load_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY v:id::INTEGER
            ORDER BY v:created_at DESC
        ) AS rn
    FROM {{ source('raw', 'customers') }}
)

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    load_timestamp
FROM ranked
WHERE rn = 1
