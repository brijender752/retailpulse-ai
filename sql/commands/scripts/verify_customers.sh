#!/usr/bin/env bash
set -euo pipefail

# Preserve Linux container arguments when running from Git Bash on Windows.
export MSYS_NO_PATHCONV=1

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

# Use the container environment, not the host shell's database credentials.
docker exec -i retailpulse-postgres sh -c \
  'exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
  < "$script_dir/../verify_customers.sql"
