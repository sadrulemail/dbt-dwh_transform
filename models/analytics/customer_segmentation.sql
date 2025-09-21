WITH customer_category_spending AS (
    SELECT
        c.customer_name,
        p.category,
        SUM(sli.line_total_amount) AS category_spending
    FROM
        {{ ref('fact_sales_line_items') }} AS sli
    JOIN
        {{ ref('dim_products') }} AS p
        ON sli.product_key = p.product_key
    JOIN
        {{ ref('fact_sales') }} AS s
        ON sli.sale_id = s.sale_id
    JOIN
        {{ ref('dim_customers') }} AS c
        ON s.customer_key = c.customer_key
    GROUP BY
        c.customer_name,
        p.category
)
SELECT
    customer_name,
    SUM(category_spending) AS total_lifetime_spending,
    ARRAY_AGG(DISTINCT category) AS favorite_categories
FROM
    customer_category_spending
GROUP BY
    customer_name
ORDER BY
    total_lifetime_spending DESC
