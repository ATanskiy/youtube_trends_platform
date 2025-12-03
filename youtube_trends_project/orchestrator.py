import pandas as pd
from youtube_client import YouTubeClient
from youtube_client_pandas import YouTubeClientPandas
from kafka_producer import KafkaProducerService
from spark_session import SparkSessionFactory
from spark_consumer import SparkKafkaConsumer
from spark_schema import SparkSchema
from settings import (
    YOUTUBE_API_KEY,
    KAFKA_BOOTSTRAP_SERVERS
)

class Orchestrator:
    def __init__(self):
        self.youtube_client = YouTubeClient(YOUTUBE_API_KEY)
        self.youtube_client_pandas = YouTubeClientPandas(YOUTUBE_API_KEY)
        self.kafka_producer = KafkaProducerService(KAFKA_BOOTSTRAP_SERVERS)

        spark_factory = SparkSessionFactory()
        self.spark = spark_factory.create_session()
        self.schema_provider = SparkSchema()
        self.spark_consumer = SparkKafkaConsumer(self.spark, self.schema_provider)

    def produce_regions_df(self):
        df = self.youtube_client_pandas.get_regions_df()
        print(df)

    def produce_regions(self):
        for region in self.youtube_client.get_regions():
            self.kafka_producer.send("youtube_regions", region.get("id"), region)

    def produce_categories(self):
        for category in self.youtube_client.get_video_categories():
            self.kafka_producer.send("youtube_categories", category.get("id"), category)

    def produce_videos(self, region_code="US", max_results=50):
        for video in self.youtube_client.get_videos(region_code, max_results=max_results):
            self.kafka_producer.send("youtube_videos", video.get("id"), video)

    def produce_comments(self, video_ids):
        for vid in video_ids:
            for comment in self.youtube_client.get_comments(vid):
                self.kafka_producer.send("youtube_comments", comment.get("id"), comment)

    def start_spark_streams(self):
        queries = []
        queries.append(self.spark_consumer.consume_regions().start())
        queries.append(self.spark_consumer.consume_categories().start())
        queries.append(self.spark_consumer.consume_videos().start())
        queries.append(self.spark_consumer.consume_comments().start())
        for q in queries:
            q.awaitTermination()
