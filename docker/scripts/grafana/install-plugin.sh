#!/bin/sh

echo "==============================================="
echo " 🚀 Starting Grafana Plugin Install Script"
echo "==============================================="
echo "Current time: $(date)"
echo "Grafana plugins directory: ${GF_PATHS_PLUGINS:-/var/lib/grafana/plugins}"
echo ""

echo "👉 Checking if Trino plugin is already installed..."
if [ -d "${GF_PATHS_PLUGINS:-/var/lib/grafana/plugins}/trino-datasource" ]; then
    echo "✔ Trino plugin already exists. Skipping install."
else
    echo "🔧 Installing Trino datasource plugin..."
    grafana cli plugins install trino-datasource

    if [ $? -eq 0 ]; then
        echo "✔ Plugin installed successfully!"
    else
        echo "❌ Plugin installation FAILED!"
        echo "Exiting."
        exit 1
    fi
fi

echo ""
echo "==============================================="
echo " 🚀 Starting Grafana Server"
echo "==============================================="
echo "Command: /run.sh"
echo "==============================================="

exec /run.sh
