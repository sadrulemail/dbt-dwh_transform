{{ config(
    materialized='incremental',
    unique_key='sale_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

SELECT
    s.sale_id,
    c.customer_key,
    s.sale_date,
    s.total_amount,
    s.status,
    s.payment_method,
    s.invoice_number
FROM
    {{ source('raw_data', 'sales') }} AS s
JOIN
    {{ ref('dim_customers') }} AS c
    ON s.customer_id = c.customer_id AND c.is_current = TRUE

{% if is_incremental() %}
  -- This filter will only load sales from the last run's max date
  WHERE s.sale_date > (SELECT MAX(sale_date) FROM {{ this }})
{% endif %}