from pyspark.sql import SparkSession
from settings import (
    CATALOG_TYPE, 
    THRIFT_HIVE_METASTORE,
    S3_ENDPOINT_URL,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    )

class SparkSessionFactory:
    """
    Creates PySpark sessions configured to write to MinIO (S3 compatible).
    """

    def create_session(self, app_name="youtube_spark_app") -> SparkSession:
        spark = (
            SparkSession.builder
            .appName(app_name)
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

        return spark