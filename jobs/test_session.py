import os
from pyspark.sql import SparkSession

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
THRIFT_HIVE_METASTORE = os.getenv("THRIFT_HIVE_METASTORE")
SPARK_WAREHOUSE_DIR = os.getenv("SPARK_WAREHOUSE_DIR")
CATALOG_TYPE = os.getenv("CATALOG_TYPE")
# --------------------------------------------------------
#  SparkSession with Iceberg + Hive + MinIO
# --------------------------------------------------------
spark = (
    SparkSession.builder
        .appName("IcebergCreateTableTest")

        # Iceberg catalog
        .config("spark.sql.catalog.youtube_trends", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.youtube_trends.type", CATALOG_TYPE)
        .config("spark.sql.catalog.youtube_trends.uri", THRIFT_HIVE_METASTORE)
        .config("spark.sql.catalog.youtube_trends.warehouse", "s3a://youtube-trends/")

        # Use S3AFileIO (NOT AWS SDK)
        .config("spark.sql.catalog.youtube_trends.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")

        # MinIO S3 endpoint settings
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT_URL)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        .getOrCreate()
)

print("✓ Spark + Iceberg catalog initialized")

# -------------------------------------------------------
# CREATE TABLE in catalog.youtube_trends schema silver
# -------------------------------------------------------

spark.sql("""
CREATE TABLE youtube_trends.silver.test_iceberg_table (
    id BIGINT,
    name STRING,
    created_at TIMESTAMP
)
USING iceberg
LOCATION 's3a://youtube-trends/silver/test_iceberg_table'
""")

print("✓ Created table youtube-trends.silver.test_iceberg_table")

# -------------------------------------------------------
# Insert 1 row to verify it works
# -------------------------------------------------------

spark.sql("""
INSERT INTO youtube_trends.silver.test_iceberg_table VALUES
(1, 'Sasha test', current_timestamp())
""")

print("✓ Inserted test row")

# -------------------------------------------------------
# Read the table
# -------------------------------------------------------

df = spark.sql("SELECT * FROM youtube_trends.silver.test_iceberg_table")
df.show()