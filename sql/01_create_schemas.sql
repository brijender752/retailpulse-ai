CREATE SCHEMA IF NOT EXISTS ecommerce;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS staging;

COMMENT ON SCHEMA ecommerce IS 'Operational e-commerce source data';
COMMENT ON SCHEMA analytics IS 'Analytics and reporting data';
COMMENT ON SCHEMA staging IS 'Temporary staging data';