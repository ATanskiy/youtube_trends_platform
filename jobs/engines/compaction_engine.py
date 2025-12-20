from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional
from pyspark.sql import SparkSession


class CompactionEngine:
    """
    Wraps Iceberg Spark procedures:
      - rewrite_data_files
      - rewrite_manifests
      - expire_snapshots
      - remove_orphan_files
    """

    def __init__(self, spark: SparkSession, catalog: str):
        self.spark = spark
        self.catalog = catalog

    def _fq_table(self, table: str) -> str:
        """
        Accepts either:
          - 'bronze.videos'  -> will become 'catalog.bronze.videos'
          - 'catalog.bronze.videos' -> kept as-is
        """
        if table.count(".") == 2:
            return table
        return f"{self.catalog}.{table}"

    def rewrite_data_files(self, table: str, target_file_size_bytes: int = 134217728) -> None:
        fq = self._fq_table(table)
        sql = f"""
        CALL {self.catalog}.system.rewrite_data_files(
          table => '{fq}',
          options => map('target-file-size-bytes', '{int(target_file_size_bytes)}')
        )
        """
        self.spark.sql(sql)

    def rewrite_manifests(self, table: str) -> None:
        fq = self._fq_table(table)
        sql = f"""
        CALL {self.catalog}.system.rewrite_manifests(
          table => '{fq}'
        )
        """
        self.spark.sql(sql)

    def expire_snapshots(self, table: str, older_than_days: int) -> None:
        fq = self._fq_table(table)
        older_than = (datetime.utcnow() - timedelta(days=int(older_than_days))).strftime("%Y-%m-%d %H:%M:%S")
        sql = f"""
        CALL {self.catalog}.system.expire_snapshots(
          table => '{fq}',
          older_than => TIMESTAMP '{older_than}'
        )
        """
        self.spark.sql(sql)

    def remove_orphans(self, table: str, older_than_hours: int = 72) -> None:
        fq = self._fq_table(table)
        older_than = (datetime.utcnow() - timedelta(hours=int(older_than_hours))).strftime("%Y-%m-%d %H:%M:%S")
        sql = f"""
        CALL {self.catalog}.system.remove_orphan_files(
          table => '{fq}',
          older_than => TIMESTAMP '{older_than}'
        )
        """
        self.spark.sql(sql)