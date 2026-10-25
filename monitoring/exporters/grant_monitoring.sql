-- Reuse the dedicated metrics reader already used by postgres-exporter.
-- Run as the PostgreSQL administrator against the monitored database.
-- This deliberately fails if the expected role does not exist.
\set ON_ERROR_STOP on
GRANT pg_monitor TO metrics_reader;
SELECT format('GRANT CONNECT ON DATABASE %I TO metrics_reader', current_database()) \gexec
