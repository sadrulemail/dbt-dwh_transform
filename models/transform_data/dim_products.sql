{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='merge',
    merge_update_columns = [
        'manufacturer_id', 'product_name', 'category', 'unit_cost',
        'unit_price', 'sku', 'is_active'
    ],
    on_schema_change='sync_all_columns'
) }}

WITH source_data AS (
    SELECT
        product_id,
        manufacturer_id,
        product_name,
        category,
        unit_cost,
        unit_price,
        sku,
        is_active,
        CURRENT_DATE() AS valid_from_date,
        CAST(NULL AS DATE) AS valid_to_date,
        TRUE AS is_current
    FROM
        {{ source('raw_data', 'products') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['product_id', 'valid_from_date']) }} AS product_key,
    product_id,
    manufacturer_id,
    product_name,
    category,
    unit_cost,
    unit_price,
    sku,
    is_active,
    valid_from_date,
    valid_to_date,
    is_current
FROM
    source_data

{% if is_incremental() %}
WHERE
    (product_id) NOT IN (SELECT product_id FROM {{ this }} WHERE is_current = TRUE) OR
    EXISTS (
        SELECT 1
        FROM {{ this }} AS T
        JOIN source_data AS S ON T.product_id = S.product_id
        WHERE T.is_current = TRUE
        AND (
            T.manufacturer_id <> S.manufacturer_id OR
            T.product_name <> S.product_name OR
            T.category <> S.category OR
            T.unit_cost <> S.unit_cost OR
            T.unit_price <> S.unit_price OR
            T.sku <> S.sku OR
            T.is_active <> S.is_active
        )
    )
{% endif %}