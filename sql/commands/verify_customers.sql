\set ON_ERROR_STOP on

-- Confirm the connected PostgreSQL role and database.
SELECT current_user, current_database();

-- Verify the source customer table is populated.
SELECT COUNT(*) AS customer_count
FROM ecommerce.customers;
