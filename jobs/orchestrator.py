from spark_session import SparkSessionFactory
from spark_consumer import SparkKafkaConsumer
from spark_schema import SparkSchema
from ddl import IcebergDDLJob


class Orchestrator:
    def __init__(self):        
        spark_factory = SparkSessionFactory()
        self.spark = spark_factory.create_session()
        self.schema_provider = SparkSchema()
        self.spark_consumer = SparkKafkaConsumer(self.spark, self.schema_provider)
        self.ddl_job = IcebergDDLJob()
    
    def start_spark_streams(self, name):
        if name == "ddl":
           self.ddl_job.run()
        if name == 'regions':
           self.spark_consumer.consume_regions()
        if name == 'languages':
           self.spark_consumer.consume_languages()
        if name == 'categories':
           self.spark_consumer.consume_categories()
        if name == 'videos':
           query = self.spark_consumer.consume_videos().start()
           query.awaitTermination()