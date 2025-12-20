import yaml
from spark_session import SparkSessionFactory
from schemas.schema_registry import SchemaRegistry


class IcebergDDLJob:
    def __init__(self):
        self.spark = SparkSessionFactory().create_session("ddl_job")
        print("🚀 Iceberg DDL Job started")

    # Helpers
    def namespace_exists(self, catalog: str, namespace: str) -> bool:
        namespaces = (
            self.spark.sql(f"SHOW NAMESPACES IN {catalog}")
            .select("namespace")
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        return namespace in namespaces

    def table_exists(self, catalog: str, namespace: str, table: str) -> bool:
        tables = (
            self.spark.sql(f"SHOW TABLES IN {catalog}.{namespace}")
            .select("tableName")
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        return table in tables

    # Namespaces
    def create_namespaces(self):
        print("\n📂 Processing namespaces")

        with open("schemas/namespaces.yml") as f:
            spec = yaml.safe_load(f)

        catalog = spec["catalog"]

        for namespace, cfg in spec["namespaces"].items():
            if self.namespace_exists(catalog, namespace):
                print(f"⚪ Namespace exists → SKIP: {catalog}.{namespace}")
                continue

            print(f"🟢 Creating namespace: {catalog}.{namespace}")
            sql = f"""
            CREATE NAMESPACE {catalog}.{namespace}
            LOCATION '{cfg["location"]}'
            """
            self.spark.sql(sql)
            print(f"✅ Created namespace: {catalog}.{namespace}")

    # Bronze tables
    def create_bronze_tables(self):
        print("\n🧱 Processing bronze tables")

        registry = SchemaRegistry("schemas/bronze.yml")
        catalog = "youtube_trends"
        namespace = registry.namespace

        for table, spec in registry.tables():
            if self.table_exists(catalog, namespace, table):
                print(f"⚪ Table exists → SKIP: {catalog}.{namespace}.{table}")
                continue

            cols = registry.columns_to_sql(spec["columns"])

            print(f"🟢 Creating table: {catalog}.{namespace}.{table}")
            sql = f"""CREATE TABLE {catalog}.{namespace}.{table} (
                {cols}
            )
            USING iceberg
            """
            print("📝 DDL:")
            print(sql.strip())
            self.spark.sql(sql)
            print(f"✅ Created table: {catalog}.{namespace}.{table}")

    # Runner
    def run(self):
        print("\n==============================")
        print("🏗  Starting Iceberg DDL setup")
        print("==============================")
        self.create_namespaces()
        self.create_bronze_tables()
        print("\n==============================")
        print("🎉 Iceberg DDL setup complete")
        print("==============================\n")


if __name__ == "__main__":
    IcebergDDLJob().run()