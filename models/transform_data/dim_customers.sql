{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge',
    merge_update_columns = [
        'customer_name', 'type', 'email', 'phone', 'address', 'city',
        'state', 'zip_code', 'is_active'
    ],
    on_schema_change='sync_all_columns'
) }}

WITH source_data AS (
    SELECT
        customer_id,
        customer_name,
        type,
        email,
        phone,
        address,
        city,
        state,
        zip_code,
        is_active,
        CURRENT_DATE() AS valid_from_date,
        CAST(NULL AS DATE) AS valid_to_date,
        TRUE AS is_current
    FROM
        {{ source('raw_data', 'customers') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'valid_from_date']) }} AS customer_key,
    customer_id,
    customer_name,
    type,
    email,
    phone,
    address,
    city,
    state,
    zip_code,
    is_active,
    valid_from_date,
    valid_to_date,
    is_current
FROM
    source_data

{% if is_incremental() %}
WHERE
    (customer_id) NOT IN (SELECT customer_id FROM {{ this }} WHERE is_current = TRUE) OR
    EXISTS (
        SELECT 1
        FROM {{ this }} AS T
        JOIN source_data AS S ON T.customer_id = S.customer_id
        WHERE T.is_current = TRUE
        AND (
            T.customer_name <> S.customer_name OR
            T.type <> S.type OR
            T.email <> S.email OR
            T.phone <> S.phone OR
            T.address <> S.address OR
            T.city <> S.city OR
            T.state <> S.state OR
            T.zip_code <> S.zip_code OR
            T.is_active <> S.is_active
        )
    )
{% endif %}