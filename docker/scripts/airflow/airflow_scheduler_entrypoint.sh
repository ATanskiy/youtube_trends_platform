#!/bin/bash
set -e

echo "⏳ [Scheduler] Waiting for Postgres to be ready..."

# Wait until Postgres is accepting connections to the airflow DB
until pg_isready -h pg_airflow_db -U admin -d airflow > /dev/null 2>&1; do
    sleep 2
done

echo "✅ [Scheduler] Postgres is ready!"

echo "🔧 [Scheduler] Running 'airflow db init' (idempotent)..."
airflow db init

echo "🔍 [Scheduler] Checking if admin user exists..."

if airflow users list --output table | grep -w "admin" > /dev/null 2>&1; then
    echo "✅ [Scheduler] Admin user already exists"
else
    echo "⚠️ [Scheduler] Admin user missing → creating..."
    airflow users create \
        --username admin \
        --password admin \
        --firstname admin \
        --lastname admin \
        --role Admin \
        --email admin@example.com || echo "⚠️ [Scheduler] Failed to create admin user (maybe already exists)"
fi

echo "🚀 [Scheduler] Starting Airflow Scheduler..."
exec airflow scheduler