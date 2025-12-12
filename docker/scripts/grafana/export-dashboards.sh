#!/bin/bash

GRAFANA_URL="http://grafana:3000"
GRAFANA_USER="admin"
GRAFANA_PASSWORD="admin"
OUTPUT_DIR="/dashboards-export"

echo "$(date): Starting dashboard export..."

# Wait for Grafana to be ready
until curl -sf -u "$GRAFANA_USER:$GRAFANA_PASSWORD" "$GRAFANA_URL/api/health" > /dev/null 2>&1; do
  echo "Waiting for Grafana..."
  sleep 2
done

# Get all dashboard UIDs from Grafana
dashboards=$(curl -s -u "$GRAFANA_USER:$GRAFANA_PASSWORD" \
  "$GRAFANA_URL/api/search?type=dash-db" | jq -r '.[] | "\(.uid)"')

# Create array of existing dashboard UIDs
existing_uids=()
for uid in $dashboards; do
  if [ -n "$uid" ] && [ "$uid" != "null" ]; then
    existing_uids+=("$uid")
  fi
done

# Export dashboards
for uid in "${existing_uids[@]}"; do
  echo "Exporting: ${uid}"
  
  curl -s -u "$GRAFANA_USER:$GRAFANA_PASSWORD" \
    "$GRAFANA_URL/api/dashboards/uid/$uid" | jq '.dashboard' \
    > "$OUTPUT_DIR/${uid}.json" 2>/dev/null
  
  if [ $? -eq 0 ]; then
    echo "✓ Exported: ${uid}.json"
  fi
done

# Clean up JSON files for deleted dashboards
echo "Checking for orphaned dashboard files..."
for json_file in "$OUTPUT_DIR"/*.json; do
  if [ -f "$json_file" ]; then
    filename=$(basename "$json_file" .json)
    
    # Skip the dashboards.yml file
    if [ "$filename" = "dashboards" ]; then
      continue
    fi
    
    # Check if this UID still exists in Grafana
    if [[ ! " ${existing_uids[@]} " =~ " ${filename} " ]]; then
      echo "🗑️  Deleting orphaned: ${json_file}"
      rm "$json_file"
    fi
  fi
done

if [ ${#existing_uids[@]} -eq 0 ]; then
  echo "No dashboards found"
fi

echo "$(date): Export completed"