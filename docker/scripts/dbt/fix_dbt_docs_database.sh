#!/bin/sh
set -e

CATALOG_NAME="youtube_trends"
TARGET_DIR="/workspace/target"

echo "🔧 Fixing dbt docs database field → ${CATALOG_NAME}"

for FILE in manifest.json catalog.json; do
  FILE_PATH="${TARGET_DIR}/${FILE}"

  if [ ! -f "$FILE_PATH" ]; then
    echo "⚠️  $FILE_PATH not found, skipping"
    continue
  fi

  echo "🛠️  Patching $FILE"

  # Replace: "database": null  → "database": "youtube_trends"
  sed -i \
    "s/\"database\": null/\"database\": \"${CATALOG_NAME}\"/g" \
    "$FILE_PATH"
done

echo "✅ dbt docs database field fixed"