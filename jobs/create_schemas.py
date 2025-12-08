import os
from pyspark.sql import SparkSession

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
THRIFT_HIVE_METASTORE = os.getenv("THRIFT_HIVE_METASTORE")
CATALOG_TYPE = os.getenv("CATALOG_TYPE")

# --------------------------------------------------------
#  SparkSession with Iceberg + Hive + MinIO
# --------------------------------------------------------
spark = (
    SparkSession.builder
        .appName("IcebergCreateTableTest")
        .config("spark.sql.catalog.youtube_trends", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.youtube_trends.type", CATALOG_TYPE)
        .config("spark.sql.catalog.youtube_trends.uri", THRIFT_HIVE_METASTORE)
        .config("spark.sql.catalog.youtube_trends.warehouse", "s3a://youtube-trends/")
        .config("spark.sql.catalog.youtube_trends.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT_URL)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
)

print("✓ Spark + Iceberg catalog initialized")

# -------------------------------------------------------
# CREATE NAMESPACE/SCHEMA FIRST
# -------------------------------------------------------
try:
    spark.sql("""
        CREATE NAMESPACE IF NOT EXISTS youtube_trends.bronze
        LOCATION 's3a://youtube-trends/bronze'
    """)
    print("✓ Created namespace youtube_trends.bronze")
except Exception as e:
    print(f"Note: {e}")

try:
    spark.sql("""
        CREATE NAMESPACE IF NOT EXISTS youtube_trends.silver
        LOCATION 's3a://youtube-trends/silver'
    """)
    print("✓ Created namespace youtube_trends.silver")
except Exception as e:
    print(f"Note: {e}")

try:
    spark.sql("""
        CREATE NAMESPACE IF NOT EXISTS youtube_trends.gold
        LOCATION 's3a://youtube-trends/gold'
    """)
    print("✓ Created namespace youtube_trends.gold")
except Exception as e:
    print(f"Note: {e}")

# show created namespaces
namespaces = spark.sql("SHOW NAMESPACES IN youtube_trends")
print("\nAvailable namespaces:")
namespaces.show()


spark.stop()