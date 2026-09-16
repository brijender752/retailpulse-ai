#!/usr/bin/env bash

set -euo pipefail

echo "======================================"
echo "Running Superset migrations"
echo "======================================"

superset db upgrade


echo "======================================"
echo "Creating Superset admin"
echo "======================================"

superset fab create-admin \
    --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
    --firstname "${SUPERSET_ADMIN_FIRSTNAME:-RetailPulse}" \
    --lastname "${SUPERSET_ADMIN_LASTNAME:-Admin}" \
    --email "${SUPERSET_ADMIN_EMAIL:-admin@retailpulse.local}" \
    --password "${SUPERSET_ADMIN_PASSWORD:-admin}" \
    || true


echo "======================================"
echo "Initializing Superset"
echo "======================================"

superset init


echo "======================================"
echo "Superset initialized successfully"
echo "======================================"