from pyspark.sql import SparkSession

# --------------------------------------------------------
#  SparkSession with Iceberg + Hive Metastore
# --------------------------------------------------------
spark = (
    SparkSession.builder
        .appName("CreateBronzeSchema")
        .getOrCreate()
)

# --------------------------------------------------------
#  Create schema "bronze" in Iceberg catalog
#  Location → s3a://iceberg/bronze/
# --------------------------------------------------------
spark.sql("""
CREATE TABLE spark_catalog.bronze.test_table (
    id INT,
    name STRING
)
USING ICEBERG
LOCATION 's3a://iceberg/bronze/test_table'
""")

print("✓ Created Iceberg schema: bronze → s3a://iceberg/bronze/")