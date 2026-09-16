SELECT
    p.product_id,

    p.product_name,

    p.category,

    p.subcategory,

    p.brand,

    p.price,

    p.unit_margin,

    p.margin_pct,

    SUM(i.quantity)
        AS units_sold,

    ROUND(
        SUM(
            i.quantity
            * i.unit_price
        ),
        2
    ) AS gross_revenue

FROM retailpulse.analytics.dim_product p

JOIN retailpulse.silver.order_items i
    ON p.product_id = i.product_id

GROUP BY
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,
    p.price,
    p.unit_margin,
    p.margin_pct

ORDER BY
    gross_revenue DESC;