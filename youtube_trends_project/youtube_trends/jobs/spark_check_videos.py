from pyspark.sql.functions import col, from_json

from config.app_settings import get_settings
from youtube_trends.infrastructure.spark.spark_session_factory import SparkSessionFactory
from youtube_trends.infrastructure.spark.spark_schema_registry import SparkSchemaRegistry


def check_videos_stream() -> None:
    settings = get_settings()
    factory = SparkSessionFactory(settings)
    spark = factory.create_streaming_session()

    video_schema = SparkSchemaRegistry.video_schema()

    df_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", settings.kafka.topic_videos)
        .option("startingOffsets", "earliest")
        .load()
    )

    df_parsed = (
        df_raw
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), video_schema).alias("data"))
        .select("data.*")
    )

    query = (
        df_parsed
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    check_videos_stream()
