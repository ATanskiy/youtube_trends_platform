from __future__ import annotations

import yaml
from typing import Dict, List, Any

from spark_session import SparkSessionFactory
from engines.compaction_engine import CompactionEngine


class CompactNamespace:
    """Compacts and maintains all Iceberg tables in a given namespace using a policy."""

    def __init__(self, spark, catalog: str, namespace: str, policy: Dict[str, Any]) -> None:
        self.spark = spark
        self.catalog = catalog
        self.namespace = namespace
        self.policy = policy
        self.compactor = CompactionEngine(self.spark, catalog=self.catalog)

    def _list_tables(self) -> List[str]:
        rows = self.spark.sql(f"SHOW TABLES IN {self.catalog}.{self.namespace}").collect()
        return [r.tableName for r in rows if not r.isTemporary]

    def run(self) -> None:
        tables = self._list_tables()
        print(f"\n📂 Namespace: {self.catalog}.{self.namespace} | tables={len(tables)}")

        if not tables:
            print("⚪ No tables found → SKIP namespace")
            return

        for t in tables:
            full_table = f"{self.namespace}.{t}"  # engine will prefix catalog
            print(f"\n🧹 Maintaining table: {self.catalog}.{full_table}")

            # 1) rewrite data files (bin-pack)
            if self.policy.get("rewrite_data_files", False):
                target_size = int(self.policy.get("target_file_size_bytes", 134217728))
                print(f"🔁 rewrite_data_files (target={target_size} bytes)")
                self.compactor.rewrite_data_files(full_table, target_file_size_bytes=target_size)
            else:
                print("⚪ rewrite_data_files → SKIP")

            # 2) rewrite manifests
            if self.policy.get("rewrite_manifests", False):
                print("📦 rewrite_manifests")
                self.compactor.rewrite_manifests(full_table)
            else:
                print("⚪ rewrite_manifests → SKIP")

            # 3) expire snapshots
            expire_days = self.policy.get("expire_snapshots_days", None)
            if expire_days is not None:
                expire_days = int(expire_days)
                print(f"🗑 expire_snapshots (older_than_days={expire_days})")
                self.compactor.expire_snapshots(full_table, older_than_days=expire_days)
            else:
                print("⚪ expire_snapshots → SKIP")

            # 4) remove orphans
            if self.policy.get("remove_orphans", False):
                older_hours = int(self.policy.get("remove_orphans_older_than_hours", 72))
                print(f"🧽 remove_orphans (older_than_hours={older_hours})")
                self.compactor.remove_orphans(full_table, older_than_hours=older_hours)
            else:
                print("⚪ remove_orphans → SKIP")

        print(f"\n✅ Maintenance for all {self.catalog}.{self.namespace} tables complete.")


class CompactionJob:
    """Loads policy YAML and runs maintenance across namespaces (bronze/silver/gold)."""

    def __init__(self, policy_path: str = "policies/compaction.yml") -> None:
        self.policy_path = policy_path
        self.spark = SparkSessionFactory().create_session("iceberg_compaction")

    def _load_policy(self) -> Dict[str, Any]:
        with open(self.policy_path, "r") as f:
            return yaml.safe_load(f)

    @staticmethod
    def _merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        merged = dict(base or {})
        merged.update(override or {})
        return merged

    def run(self) -> None:
        spec = self._load_policy()
        catalog = spec["catalog"]

        defaults = spec.get("defaults", {})
        ns_policies = spec.get("namespaces", {})
        table_overrides = spec.get("tables", {})

        # The namespaces we want to process:
        namespaces = list(ns_policies.keys()) if ns_policies else ["bronze", "silver", "gold"]

        print("\n==============================")
        print("🧹 Starting Iceberg Compaction")
        print(f"Catalog: {catalog}")
        print(f"Namespaces: {namespaces}")
        print("==============================\n")

        for ns in namespaces:
            # merge defaults + namespace policy
            policy = self._merge_dicts(defaults, ns_policies.get(ns, {}))

            # Optional: if you want per-table overrides, we apply them inside CompactNamespace per table.
            # For now, we keep per-table overrides in the job by wrapping CompactNamespace's run logic.
            # We'll apply overrides by temporarily patching policy per table at runtime.
            # Easiest: run CompactNamespace but resolve overrides by re-running logic here:

            compact = CompactNamespace(self.spark, catalog=catalog, namespace=ns, policy=policy)

            # If no per-table overrides exist, run directly
            if not table_overrides:
                compact.run()
                continue

            # With per-table overrides: do per-table policies
            tables = compact._list_tables()
            print(f"\n📂 Namespace: {catalog}.{ns} | tables={len(tables)}")

            if not tables:
                print("⚪ No tables found → SKIP namespace")
                continue

            for t in tables:
                key = f"{ns}.{t}"
                per_table = table_overrides.get(key, {})
                effective_policy = self._merge_dicts(policy, per_table)

                # run a single-table maintenance using CompactNamespace-like logic
                print(f"\n🧹 Maintaining table: {catalog}.{ns}.{t} (policy override={bool(per_table)})")

                full_table = f"{ns}.{t}"
                if effective_policy.get("rewrite_data_files", False):
                    target_size = int(effective_policy.get("target_file_size_bytes", 134217728))
                    print(f"🔁 rewrite_data_files (target={target_size} bytes)")
                    compact.compactor.rewrite_data_files(full_table, target_file_size_bytes=target_size)
                else:
                    print("⚪ rewrite_data_files → SKIP")

                if effective_policy.get("rewrite_manifests", False):
                    print("📦 rewrite_manifests")
                    compact.compactor.rewrite_manifests(full_table)
                else:
                    print("⚪ rewrite_manifests → SKIP")

                expire_days = effective_policy.get("expire_snapshots_days", None)
                if expire_days is not None:
                    expire_days = int(expire_days)
                    print(f"🗑 expire_snapshots (older_than_days={expire_days})")
                    compact.compactor.expire_snapshots(full_table, older_than_days=expire_days)
                else:
                    print("⚪ expire_snapshots → SKIP")

                if effective_policy.get("remove_orphans", False):
                    older_hours = int(effective_policy.get("remove_orphans_older_than_hours", 72))
                    print(f"🧽 remove_orphans (older_than_hours={older_hours})")
                    compact.compactor.remove_orphans(full_table, older_than_hours=older_hours)
                else:
                    print("⚪ remove_orphans → SKIP")

            print(f"\n✅ Maintenance for all {catalog}.{ns} tables complete.")

        print("\n==============================")
        print("🎉 Iceberg Compaction complete")
        print("==============================\n")


if __name__ == "__main__":
    CompactionJob().run()