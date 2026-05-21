{{ config(materialized='view') }}

WITH ranked AS (
    SELECT
        v:id::INTEGER           AS account_id,
        v:customer_id::INTEGER   AS customer_id,
        v:account_type::STRING  AS account_type,
        v:balance::FLOAT        AS balance,
        v:currency::STRING       AS currency,
        v:created_at::TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP       AS load_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY v:id::INTEGER
            ORDER BY v:created_at DESC
        ) AS rn
    FROM {{ source('raw', 'accounts') }}
)

SELECT 
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at,
    load_timestamp
FROM ranked
WHERE rn = 1