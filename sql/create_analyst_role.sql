-- Run with psql as a database administrator connected to retailpulse.
-- First create ai_reporting.customer_metrics, owned by a trusted role with
-- the required underlying table permissions. Use the view's default owner
-- permission checks (not security_invoker).
-- This script intentionally fails if the analyst role already exists.
\set ON_ERROR_STOP on

BEGIN;

CREATE ROLE retailpulse_analyst LOGIN;

-- psql prompts without echoing the password and encrypts it before sending.
-- Supply a strong password here; never put it in this file or a shell command.
\password retailpulse_analyst

GRANT CONNECT ON DATABASE retailpulse
TO retailpulse_analyst;

GRANT USAGE ON SCHEMA ai_reporting
TO retailpulse_analyst;

GRANT SELECT ON ai_reporting.customer_metrics
TO retailpulse_analyst;

ALTER ROLE retailpulse_analyst
SET default_transaction_read_only = on;

-- Approve future reporting views individually with explicit SELECT grants.
-- Do not grant privileges on serving, ecommerce, or other underlying schemas.

COMMIT;
