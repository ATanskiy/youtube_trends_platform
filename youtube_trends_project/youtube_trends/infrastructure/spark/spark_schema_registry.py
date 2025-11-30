from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
)


class SparkSchemaRegistry:
    """Holds StructType schemas for different data objects."""

    @staticmethod
    def video_schema() -> StructType:
        return StructType([
            StructField("video_id", StringType(), False),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("channel_id", StringType(), True),
            StructField("channel_title", StringType(), True),
            StructField("category_id", StringType(), True),
            StructField("published_at", TimestampType(), True),
            StructField("region_code", StringType(), True),
            StructField("view_count", LongType(), True),
            StructField("like_count", LongType(), True),
            StructField("comment_count", LongType(), True),
        ])

    @staticmethod
    def category_schema() -> StructType:
        return StructType([
            StructField("category_id", StringType(), False),
            StructField("title", StringType(), False),
            StructField("assignable", StringType(), True),
        ])

    @staticmethod
    def region_schema() -> StructType:
        return StructType([
            StructField("region_code", StringType(), False),
            StructField("name", StringType(), False),
        ])

    @staticmethod
    def comment_schema() -> StructType:
        return StructType([
            StructField("comment_id", StringType(), False),
            StructField("video_id", StringType(), False),
            StructField("author", StringType(), True),
            StructField("text", StringType(), True),
            StructField("like_count", LongType(), True),
            StructField("published_at", TimestampType(), True),
        ])
