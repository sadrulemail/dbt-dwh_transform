SELECT
    DATE_TRUNC('month', s.sale_date) AS sales_month,
    p.category,
    i.warehouse_location,
    SUM(sli.quantity) AS total_units_sold,
    SUM(sli.line_total_amount) AS total_revenue,
    SUM(sli.quantity * p.unit_cost) AS total_cost_of_goods_sold,
    (SUM(sli.line_total_amount) - SUM(sli.quantity * p.unit_cost)) AS total_profit
FROM
    {{ ref('fact_sales_line_items') }} AS sli
JOIN
    {{ ref('dim_products') }} AS p
    ON sli.product_key = p.product_key
JOIN
    {{ ref('dim_inventory') }} AS i
    ON p.product_id = i.product_id AND i.is_current = TRUE
JOIN
    {{ ref('fact_sales') }} AS s
    ON sli.sale_id = s.sale_id
GROUP BY
    sales_month,
    p.category,
    i.warehouse_location
ORDER BY
    sales_month,
    total_revenue DESC