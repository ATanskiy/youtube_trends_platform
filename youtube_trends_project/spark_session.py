from pyspark.sql import SparkSession
from settings import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, AWS_REGION

class SparkSessionFactory:
    """
    Creates PySpark sessions configured to write to MinIO (S3 compatible).
    """

    def create_session(self, app_name="youtube_spark_app") -> SparkSession:
        spark = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")  # ensure Hadoop AWS package
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")  # required for MinIO
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.DirectParquetOutputCommitter")
            .getOrCreate()
        )

        return spark
