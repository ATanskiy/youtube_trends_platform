from pyspark.sql import SparkSession

from config.app_settings import AppSettings


class SparkSessionFactory:
    """Factory for creating SparkSession instances."""

    def __init__(self, settings: AppSettings):
        self._settings = settings

    def create_batch_session(self) -> SparkSession:
        return (
            SparkSession.builder
            .appName(self._settings.spark.app_name)
            .master(self._settings.spark.master)
            .getOrCreate()
        )

    def create_streaming_session(self) -> SparkSession:
        return (
            SparkSession.builder
            .appName(self._settings.spark.app_name + "_streaming")
            .master(self._settings.spark.master)
            .config(
                "spark.jars.packages",
                self._settings.spark.kafka_package,
            )
            .getOrCreate()
        )
