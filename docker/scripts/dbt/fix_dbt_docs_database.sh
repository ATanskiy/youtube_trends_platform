#!/bin/bash
set -e

CATALOG_NAME="youtube_trends"
TARGET_DIR="/workspace/target"

echo "🔧 Fixing dbt docs: database + schema-aware lineage + BI overrides"

# Check if target directory exists
if [ ! -d "$TARGET_DIR" ]; then
    echo "❌ Target directory $TARGET_DIR not found"
    exit 1
fi

python3 <<'EOF'
import json
import sys
import os

CATALOG_NAME = "youtube_trends"
TARGET_DIR = "/workspace/target"

# Schemas that should be prefixed in docs
SCHEMAS_TO_PREFIX = {"silver", "gold"}

# Special BI model
BI_MODEL_NAME = "pg_videos_enriched_tableau"
BI_SCHEMA = "pg_bi"

def fix_manifest():
    filepath = os.path.join(TARGET_DIR, "manifest.json")

    if not os.path.exists(filepath):
        print("⚠️  manifest.json not found")
        return False

    print("🛠️  Processing manifest.json")

    with open(filepath, "r") as f:
        manifest = json.load(f)

    changes = 0

    for node_key, node in manifest.get("nodes", {}).items():
        resource_type = node.get("resource_type")
        name = node.get("name")
        schema = node.get("schema")

        if resource_type not in {"model", "seed"}:
            continue

        # Ensure database/catalog is set
        if node.get("database") is None:
            node["database"] = CATALOG_NAME
            changes += 1

        # ==========================
        # 🔥 SPECIAL CASE: BI TABLE
        # ==========================
        if resource_type == "model" and name == BI_MODEL_NAME:
            node["schema"] = BI_SCHEMA
            node["name"] = f"{BI_SCHEMA}.{BI_MODEL_NAME}"
            node["alias"] = BI_MODEL_NAME
            node["relation_name"] = f"{CATALOG_NAME}.{BI_SCHEMA}.{BI_MODEL_NAME}"
            changes += 1
            continue

        # ==========================
        # Regular silver / gold models
        # ==========================
        if resource_type == "model" and schema in SCHEMAS_TO_PREFIX and name:
            if not name.startswith(f"{schema}."):
                node["name"] = f"{schema}.{name}"
                node.setdefault("alias", name.split(".")[-1])
                changes += 1

        # Always fix relation_name
        if schema and name:
            base_name = name.split(".")[-1]
            node["relation_name"] = f"{CATALOG_NAME}.{schema}.{base_name}"

    # ==========================
    # Fix sources
    # ==========================
    for source_key, source in manifest.get("sources", {}).items():
        if source.get("database") is None:
            source["database"] = CATALOG_NAME
            changes += 1

        schema = source.get("schema")
        name = source.get("name")

        if schema in SCHEMAS_TO_PREFIX and name:
            if not name.startswith(f"{schema}."):
                source["name"] = f"{schema}.{name}"
                changes += 1

        if schema and name:
            base_name = name.split(".")[-1]
            source["relation_name"] = f"{CATALOG_NAME}.{schema}.{base_name}"

    with open(filepath, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"   ✓ Updated manifest.json ({changes} changes)")
    return True


def fix_catalog():
    filepath = os.path.join(TARGET_DIR, "catalog.json")

    if not os.path.exists(filepath):
        print("⚠️  catalog.json not found")
        return False

    print("🛠️  Processing catalog.json")

    with open(filepath, "r") as f:
        catalog = json.load(f)

    changes = 0

    for node in catalog.get("nodes", {}).values():
        node.setdefault("metadata", {})
        if node["metadata"].get("database") is None:
            node["metadata"]["database"] = CATALOG_NAME
            changes += 1

    for source in catalog.get("sources", {}).values():
        source.setdefault("metadata", {})
        if source["metadata"].get("database") is None:
            source["metadata"]["database"] = CATALOG_NAME
            changes += 1

    with open(filepath, "w") as f:
        json.dump(catalog, f, indent=2)

    print(f"   ✓ Updated catalog.json ({changes} changes)")
    return True


success = True
success &= fix_manifest()
success &= fix_catalog()

if success:
    print("✅ dbt docs lineage fixed successfully")
    sys.exit(0)
else:
    print("⚠️  dbt docs fix incomplete")
    sys.exit(1)
EOF

echo "✅ Done"