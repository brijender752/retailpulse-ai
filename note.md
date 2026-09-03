docker exec -it retailpulse-postgres psql -U retailpulse -d retailpulse

SELECT 'customers' AS table_name, COUNT(*) FROM ecommerce.customers
UNION ALL
SELECT 'products', COUNT(*) FROM ecommerce.products
UNION ALL
SELECT 'orders', COUNT(*) FROM ecommerce.orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM ecommerce.order_items
UNION ALL
SELECT 'payments', COUNT(*) FROM ecommerce.payments
UNION ALL
SELECT 'inventory', COUNT(*) FROM ecommerce.inventory
UNION ALL
SELECT 'website_events', COUNT(*) FROM ecommerce.website_events
UNION ALL
SELECT 'support_tickets', COUNT(*) FROM ecommerce.support_tickets
UNION ALL
SELECT 'marketing_events', COUNT(*) FROM ecommerce.marketing_events;

SELECT COUNT(*)
FROM ecommerce.orders o
LEFT JOIN ecommerce.customers c
    ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

SELECT COUNT(*)
FROM ecommerce.order_items oi
LEFT JOIN ecommerce.orders o
    ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;

SELECT COUNT(*)
FROM ecommerce.order_items oi
LEFT JOIN ecommerce.products p
    ON oi.product_id = p.product_id
WHERE p.product_id IS NULL;

SELECT status, COUNT(*) AS order_count
FROM ecommerce.orders
GROUP BY status
ORDER BY order_count DESC;

SELECT payment_status, COUNT(*) AS payment_count
FROM ecommerce.payments
GROUP BY payment_status
ORDER BY payment_count DESC;

SELECT event_type, COUNT(*) AS event_count
FROM ecommerce.website_events
GROUP BY event_type
ORDER BY event_count DESC;


SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    COUNT(o.order_id) AS total_orders,
    COALESCE(SUM(o.total_amount), 0) AS total_spend
FROM ecommerce.customers c
LEFT JOIN ecommerce.orders o
    ON c.customer_id = o.customer_id
GROUP BY
    c.customer_id,
    c.first_name,
    c.last_name
ORDER BY total_spend DESC
LIMIT 10;

TRUNCATE TABLE
    ecommerce.marketing_events,
    ecommerce.support_tickets,
    ecommerce.website_events,
    ecommerce.inventory,
    ecommerce.payments,
    ecommerce.order_items,
    ecommerce.orders,
    ecommerce.products,
    ecommerce.customers
RESTART IDENTITY CASCADE;