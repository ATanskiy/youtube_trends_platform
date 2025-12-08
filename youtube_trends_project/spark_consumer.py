from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from settings import (
    KAFKA_TOPIC_REGIONS,
    KAFKA_TOPIC_CATEGORIES,
    KAFKA_TOPIC_VIDEOS,
    KAFKA_TOPIC_COMMENTS,
    MINIO_BUCKET_PREFIX,
    KAFKA_BOOTSTRAP_SERVERS
)
from spark_schema import SparkSchema

class SparkKafkaConsumer:
    def __init__(self, spark: SparkSession, schema_provider: SparkSchema):
        self.spark = spark
        self.schema_provider = schema_provider

    def _read_kafka(self, topic):
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .load()
        )

    def _parse_json(self, df, schema):
        return df.selectExpr("CAST(value AS STRING)") \
                 .select(from_json(col("value"), schema).alias("data")) \
                 .select("data.*")

    def _write_to_minio(self, df, path): 
        return df.writeStream \
            .format("parquet") \
            .option("path", f"{MINIO_BUCKET_PREFIX}/{path}") \
            .option("checkpointLocation", f"{MINIO_BUCKET_PREFIX}/{path}/_checkpoint") \
            .outputMode("append") \
            .trigger(processingTime="15 seconds")

    def consume_regions(self):
        df = self._read_kafka(KAFKA_TOPIC_REGIONS)
        parsed = self._parse_json(df, self.schema_provider.region_schema())
        return self._write_to_minio(parsed, "regions")

    def consume_categories(self):
        df = self._read_kafka(KAFKA_TOPIC_CATEGORIES)
        parsed = self._parse_json(df, self.schema_provider.category_schema())
        return self._write_to_minio(parsed, "categories")

    def consume_videos(self):
        df = self._read_kafka(KAFKA_TOPIC_VIDEOS)
        parsed = self._parse_json(df, self.schema_provider.videos_schema())
        return self._write_to_minio(parsed, "videos")

    def consume_comments(self):
        df = self._read_kafka(KAFKA_TOPIC_COMMENTS)
        parsed = self._parse_json(df, self.schema_provider.comments_schema())
        return self._write_to_minio(parsed, "comments")
