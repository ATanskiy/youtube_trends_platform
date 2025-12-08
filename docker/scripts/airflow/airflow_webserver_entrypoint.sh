#!/bin/bash
set -e

echo "⏳ [Webserver] Waiting for Postgres to be ready..."
until pg_isready -h pg_airflow_db -U admin -d airflow > /dev/null 2>&1; do
    sleep 2
done

echo "⭐ [Webserver] Postgres is ready!"

echo "ℹ️ [Webserver] Skipping DB migrations — Scheduler owns this."

echo "🚀 [Webserver] Starting Airflow Webserver..."
exec airflow webserver