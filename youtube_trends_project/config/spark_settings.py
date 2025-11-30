from pydantic import BaseSettings, Field


class SparkSettings(BaseSettings):
    app_name: str = Field("youtube_trends_spark", env="SPARK_APP_NAME")
    master: str = Field("local[*]", env="SPARK_MASTER")
    kafka_package: str = Field(
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        env="SPARK_KAFKA_PACKAGE",
    )
