{{ config(
    materialized='incremental',
    unique_key='manufacturer_id',
    incremental_strategy='merge',
    merge_update_columns = [
        'manufacturer_name', 'country', 'contact_email', 'phone', 'is_active'
    ],
    on_schema_change='sync_all_columns'
) }}

WITH source_data AS (
    SELECT
        manufacturer_id,
        manufacturer_name,
        country,
        contact_email,
        phone,
        is_active,
        CURRENT_DATE() AS valid_from_date,
        CAST(NULL AS DATE) AS valid_to_date,
        TRUE AS is_current
    FROM
        {{ source('raw_data', 'manufacturers') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['manufacturer_id', 'valid_from_date']) }} AS manufacturer_key,
    manufacturer_id,
    manufacturer_name,
    country,
    contact_email,
    phone,
    is_active,
    valid_from_date,
    valid_to_date,
    is_current
FROM
    source_data

{% if is_incremental() %}
WHERE
    (manufacturer_id) NOT IN (SELECT manufacturer_id FROM {{ this }} WHERE is_current = TRUE) OR
    EXISTS (
        SELECT 1
        FROM {{ this }} AS T
        JOIN source_data AS S ON T.manufacturer_id = S.manufacturer_id
        WHERE T.is_current = TRUE
        AND (
            T.manufacturer_name <> S.manufacturer_name OR
            T.country <> S.country OR
            T.contact_email <> S.contact_email OR
            T.phone <> S.phone OR
            T.is_active <> S.is_active
        )
    )
{% endif %}