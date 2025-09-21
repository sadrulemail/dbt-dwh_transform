SELECT
    p.product_name,
    m.manufacturer_name,
    c.type AS customer_type,
    SUM(sli.line_total_amount) AS total_revenue
FROM
    {{ ref('fact_sales_line_items') }} AS sli
JOIN
    {{ ref('dim_products') }} AS p
    ON sli.product_key = p.product_key
JOIN
    {{ ref('dim_manufacturers') }} AS m
    ON p.manufacturer_id = m.manufacturer_id
JOIN
    {{ ref('fact_sales') }} AS s
    ON sli.sale_id = s.sale_id
JOIN
    {{ ref('dim_customers') }} AS c
    ON s.customer_key = c.customer_key
GROUP BY
    p.product_name,
    m.manufacturer_name,
    customer_type
ORDER BY
    total_revenue DESC