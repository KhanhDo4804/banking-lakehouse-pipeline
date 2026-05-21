{{ config(materialized='view') }}

SELECT
    v:id::INTEGER                   as transaction_id,
    v:account_id::INTEGER           as account_id,
    v:txn_type::STRING              as transaction_type,
    v:amount::FLOAT                 as amount,
    v:related_account_id::INTEGER   as related_account_id,
    v:status::STRING                as status,
    v:created_at::TIMESTAMP         as transaction_time,
    CURRENT_TIMESTAMP               as load_timestamp
FROM {{ source('raw', 'transactions') }}
