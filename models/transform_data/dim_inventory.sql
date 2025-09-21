{{ config(
    materialized='incremental',
    unique_key='inventory_id',
    incremental_strategy='merge',
    merge_update_columns = [
        'product_id', 'quantity_on_hand', 'low_stock_threshold',
        'last_restocked_date', 'warehouse_location'
    ],
    on_schema_change='sync_all_columns'
) }}

WITH source_data AS (
    SELECT
        inventory_id,
        product_id,
        quantity_on_hand,
        low_stock_threshold,
        last_restocked_date,
        warehouse_location,
        CURRENT_DATE() AS valid_from_date,
        CAST(NULL AS DATE) AS valid_to_date,
        TRUE AS is_current
    FROM
        {{ source('raw_data', 'inventory') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['inventory_id', 'valid_from_date']) }} AS inventory_key,
    inventory_id,
    product_id,
    quantity_on_hand,
    low_stock_threshold,
    last_restocked_date,
    warehouse_location,
    valid_from_date,
    valid_to_date,
    is_current
FROM
    source_data

{% if is_incremental() %}
WHERE
    (inventory_id) NOT IN (SELECT inventory_id FROM {{ this }} WHERE is_current = TRUE) OR
    EXISTS (
        SELECT 1
        FROM {{ this }} AS T
        JOIN source_data AS S ON T.inventory_id = S.inventory_id
        WHERE T.is_current = TRUE
        AND (
            T.product_id <> S.product_id OR
            T.quantity_on_hand <> S.quantity_on_hand OR
            T.low_stock_threshold <> S.low_stock_threshold OR
            T.last_restocked_date <> S.last_restocked_date OR
            T.warehouse_location <> S.warehouse_location
        )
    )
{% endif %}