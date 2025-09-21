{{ config(
    materialized='incremental',
    unique_key='sale_item_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

-- depends_on: {{ ref('fact_sales') }}

SELECT
    sli.sale_item_id,
    sli.sale_id,
    p.product_key,
    sli.quantity,
    sli.unit_price,
    (sli.quantity * sli.unit_price) as line_total_amount
FROM
    {{ source('raw_data', 'sales_line_items') }} AS sli
JOIN
    {{ ref('dim_products') }} AS p
    ON sli.product_id = p.product_id AND p.is_current = TRUE
JOIN
    {{ source('raw_data', 'sales') }} as s
    ON sli.sale_id = s.sale_id

{% if is_incremental() %}
  -- This filter ensures we only process new line items associated with new sales
  WHERE s.sale_date > (SELECT MAX(sale_date) FROM {{ ref('fact_sales') }})
{% endif %}