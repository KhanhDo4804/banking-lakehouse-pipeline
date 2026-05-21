{{ config(materialized='incremental', unique_key='transaction_id') }}

SELECT
    t.transaction_id,
    t.account_id,
    a.customer_id,
    t.amount,
    t.related_account_id,
    t.status,
    t.transaction_type,
    t.transaction_time,
    CURRENT_TIMESTAMP AS load_timestamp
FROM {{ ref('stg_transaction') }} as t
LEFT JOIN {{ ref('stg_account') }} AS a
    ON t.account_id = a.account_id