#!/bin/bash
set -e

echo "=== PostgreSQL Initialization Script Starting ==="
echo "Creating Airflow database for user: $POSTGRES_USER"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<EOSQL
DO \$\$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow') THEN
      CREATE DATABASE airflow;
   END IF;
END
\$\$;

GRANT ALL PRIVILEGES ON DATABASE airflow TO $POSTGRES_USER;
ALTER USER $POSTGRES_USER CREATEDB;
EOSQL

echo "=== Airflow database created successfully! ==="
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "\l"
echo "=== PostgreSQL Initialization Script Completed ==="