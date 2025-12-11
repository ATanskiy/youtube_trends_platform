# import json
# import isodate
# import pandas as pd
from spark_session import SparkSessionFactory
from spark_consumer import SparkKafkaConsumer
from spark_schema import SparkSchema
from settings import (
    YOUTUBE_API_KEY,
    KAFKA_BOOTSTRAP_SERVERS,
)


class Orchestrator:
    def __init__(self):        
        spark_factory = SparkSessionFactory()
        self.spark = spark_factory.create_session()
        self.schema_provider = SparkSchema()
        self.spark_consumer = SparkKafkaConsumer(self.spark, self.schema_provider)
    
    def start_spark_streams(self, name):
        queries = []

        if name == 'regions':
           queries.append(self.spark_consumer.consume_regions().start())
        if name == 'categories':
           queries.append(self.spark_consumer.consume_categories().start())
        if name == 'videos':
           queries.append(self.spark_consumer.consume_videos().start())
        # if name == 'comments':
        # queries.append(self.spark_consumer.consume_comments().start())
        for q in queries:
            q.awaitTermination()
