#!/bin/bash
set -e

echo "🚀 Initializing BI database..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

-- Create schemas
CREATE SCHEMA IF NOT EXISTS bi;

-- Optional: permissions (dbt & BI tools)
GRANT ALL PRIVILEGES ON SCHEMA bi TO $POSTGRES_USER;

-- Default search path
ALTER ROLE $POSTGRES_USER SET search_path = bi, public;

EOSQL

echo "✅ BI database initialized successfully"