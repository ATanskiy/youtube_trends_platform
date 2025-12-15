#!/bin/bash
set -e

CATALOG_NAME="youtube_trends"
TARGET_DIR="/workspace/target"

echo "🔧 Fixing dbt docs: database + schema-aware lineage"

# Check if target directory exists
if [ ! -d "$TARGET_DIR" ]; then
    echo "❌ Target directory $TARGET_DIR not found"
    exit 1
fi

# Process both manifest.json and catalog.json
python3 <<'EOF'
import json
import sys
import os

CATALOG_NAME = "youtube_trends"
TARGET_DIR = "/workspace/target"

# Only apply schema prefixes to these schemas
SCHEMAS_TO_PREFIX = {"silver", "gold"}

def fix_manifest():
    """Fix manifest.json: set database and update node names for lineage"""
    filepath = os.path.join(TARGET_DIR, "manifest.json")
    
    if not os.path.exists(filepath):
        print(f"⚠️  manifest.json not found")
        return False
    
    print("🛠️  Processing manifest.json")
    
    with open(filepath, "r") as f:
        manifest = json.load(f)
    
    changes = 0
    
    # Fix nodes (models and seeds)
    for node_key, node in manifest.get("nodes", {}).items():
        resource_type = node.get("resource_type")
        
        # Process models and seeds
        if resource_type in ["model", "seed"]:
            # Set database
            if node.get("database") is None:
                node["database"] = CATALOG_NAME
                changes += 1
            
            schema = node.get("schema")
            name = node.get("name")
            
            # Only add schema prefix for silver and gold MODELS (not seeds)
            if resource_type == "model" and schema in SCHEMAS_TO_PREFIX and name:
                # Check if name doesn't already have schema prefix
                if not name.startswith(f"{schema}."):
                    # Update name to include schema for lineage display
                    node["name"] = f"{schema}.{name}"
                    
                    # Keep original name as alias
                    if "alias" not in node:
                        node["alias"] = name.split('.')[-1]  # Original table name
                    
                    changes += 1
            
            # Always update relation_name to full 3-part name
            if schema and name:
                table_name = name.split('.')[-1]  # Get base name without prefix
                node["relation_name"] = f"{CATALOG_NAME}.{schema}.{table_name}"
    
    # Fix sources
    for source_key, source in manifest.get("sources", {}).items():
        # Set database
        if source.get("database") is None:
            source["database"] = CATALOG_NAME
            changes += 1
        
        schema = source.get("schema")
        name = source.get("name")
        
        # Only add schema prefix for silver and gold sources
        if schema in SCHEMAS_TO_PREFIX and name:
            if not name.startswith(f"{schema}."):
                source["name"] = f"{schema}.{name}"
                changes += 1
        
        # Update relation_name
        if schema and name:
            table_name = name.split('.')[-1]
            source["relation_name"] = f"{CATALOG_NAME}.{schema}.{table_name}"
    
    # Write back
    with open(filepath, "w") as f:
        json.dump(manifest, f, indent=2)
    
    print(f"   ✓ Made {changes} changes to manifest.json")
    print(f"   ℹ️  Schema prefixes applied only to: {', '.join(SCHEMAS_TO_PREFIX)}")
    return True

def fix_catalog():
    """Fix catalog.json: set database fields"""
    filepath = os.path.join(TARGET_DIR, "catalog.json")
    
    if not os.path.exists(filepath):
        print(f"⚠️  catalog.json not found")
        return False
    
    print("🛠️  Processing catalog.json")
    
    with open(filepath, "r") as f:
        catalog = json.load(f)
    
    changes = 0
    
    # Fix all nodes in catalog
    for node_key, node in catalog.get("nodes", {}).items():
        if node.get("metadata", {}).get("database") is None:
            if "metadata" not in node:
                node["metadata"] = {}
            node["metadata"]["database"] = CATALOG_NAME
            changes += 1
    
    # Fix sources in catalog
    for source_key, source in catalog.get("sources", {}).items():
        if source.get("metadata", {}).get("database") is None:
            if "metadata" not in source:
                source["metadata"] = {}
            source["metadata"]["database"] = CATALOG_NAME
            changes += 1
    
    # Write back
    with open(filepath, "w") as f:
        json.dump(catalog, f, indent=2)
    
    print(f"   ✓ Made {changes} changes to catalog.json")
    return True

# Process both files
success = True
if not fix_manifest():
    success = False
if not fix_catalog():
    success = False

if success:
    print("✅ Database + schema-aware lineage fixed successfully")
    sys.exit(0)
else:
    print("⚠️  Some files could not be processed")
    sys.exit(1)
EOF

echo "✅ Done"