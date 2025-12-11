import traceback
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.functions import from_json, col
from settings import (
    KAFKA_TOPIC_REGIONS,
    KAFKA_TOPIC_CATEGORIES,
    KAFKA_TOPIC_VIDEOS,
    KAFKA_TOPIC_COMMENTS,
    MINIO_BUCKET_PREFIX,
    KAFKA_BOOTSTRAP_SERVERS,
    MINIO_YOUTUBE_TRENDS_BUCKET,
    MINIO_YOUTUBE_TRENDS_CATALOG,
    MINIO_BRONZE_NAMESPACE,
    MINIO_SILVER_NAMESPACE,
    MINIO_GOLD_NAMESPACE,
    MINIO_REGIONS_TABLE,
    MINIO_CATEGORIES_TABLE,
    MINIO_VIDEOS_TABLE
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
                      
    def write_regions_to_iceberg(self, df, batch_id):
        try:
            row_count = df.count()
            print("=" * 80)
            print(f"🔥 Writing batch {batch_id} → Iceberg table: {MINIO_YOUTUBE_TRENDS_CATALOG}.{MINIO_BRONZE_NAMESPACE}.{MINIO_REGIONS_TABLE}")
            print(f"Rows: {row_count}")
            print("=" * 80)

            if row_count > 0:
                (
                    df.writeTo(f"{MINIO_YOUTUBE_TRENDS_CATALOG}.{MINIO_BRONZE_NAMESPACE}.{MINIO_REGIONS_TABLE}")
                    .append()
                )
        except Exception:
            print("❌ ERROR in write_to_iceberg()")
            traceback.print_exc()

    def write_categories_to_iceberg(self, df, batch_id):
        try:
            row_count = df.count()
            print("=" * 80)
            print(f"🔥 Writing batch {batch_id} → Iceberg table: {MINIO_YOUTUBE_TRENDS_CATALOG}.{MINIO_BRONZE_NAMESPACE}.{MINIO_CATEGORIES_TABLE}")
            print(f"Rows: {row_count}")
            print("=" * 80)

            if row_count > 0:
                (
                    df.writeTo(f"{MINIO_YOUTUBE_TRENDS_CATALOG}.{MINIO_BRONZE_NAMESPACE}.{MINIO_CATEGORIES_TABLE}")
                    .append()
                )
        except Exception:
            print("❌ ERROR in write_to_iceberg()")
            traceback.print_exc()
    
    def write_videos_to_iceberg(self, df, batch_id):
        try:
            row_count = df.count()
            print("=" * 80)
            print(f"🔥 Writing batch {batch_id} → Iceberg table: {MINIO_YOUTUBE_TRENDS_CATALOG}.{MINIO_BRONZE_NAMESPACE}.{MINIO_VIDEOS_TABLE}")
            print(f"Rows: {row_count}")
            print("=" * 80)

            if row_count > 0:
                (
                    df.writeTo(f"{MINIO_YOUTUBE_TRENDS_CATALOG}.{MINIO_BRONZE_NAMESPACE}.{MINIO_VIDEOS_TABLE}")
                    .append()
                )
        except Exception:
            print("❌ ERROR in write_to_iceberg()")
            traceback.print_exc()
    
    def _write_to_iceberg(self, df, namespace, table):        
        if table == "regions":
            return (
                df.writeStream.foreachBatch(self.write_regions_to_iceberg)
                .outputMode("update")
                .option("checkpointLocation", f"s3a://{MINIO_YOUTUBE_TRENDS_BUCKET}/checkpoints/{namespace}/{table}")
                .option("startingOffsets", "latest")                
                .trigger(processingTime="10 seconds")
            )
        
        if table == "categories":                
            return (
            df.writeStream.foreachBatch(self.write_categories_to_iceberg)
            .outputMode("update")
            .option("checkpointLocation", f"s3a://{MINIO_YOUTUBE_TRENDS_BUCKET}/checkpoints/{namespace}/{table}")
            .option("startingOffsets", "latest")                
            .trigger(processingTime="10 seconds")
            )
        
        if table == "videos":    
            return (
            df.writeStream.foreachBatch(self.write_videos_to_iceberg)
            .outputMode("append")
            .option("checkpointLocation", f"s3a://{MINIO_YOUTUBE_TRENDS_BUCKET}/checkpoints/{namespace}/{table}")
            .option("startingOffsets", "latest")                
            .trigger(processingTime="10 seconds")
            )


    def _write_to_minio(self, df, path): 
        return df.writeStream \
            .format("parquet") \
            .option("path", f"{MINIO_BUCKET_PREFIX}/{path}") \
            .option("checkpointLocation", f"s3a:///{path}/_checkpoint") \
            .outputMode("append") \
            .trigger(processingTime="15 seconds")


    def consume_regions(self):
        schema_str = self.schema_provider.struct_to_schema_string(self.schema_provider.region_schema(), multiline=True)
        print(schema_str)
        self.create_table(schema_str, MINIO_YOUTUBE_TRENDS_CATALOG, MINIO_BRONZE_NAMESPACE, MINIO_REGIONS_TABLE)
        
        df = self._read_kafka(KAFKA_TOPIC_REGIONS)        
        parsed = self._parse_json(df, self.schema_provider.region_schema())
        return self._write_to_iceberg(parsed, MINIO_BRONZE_NAMESPACE, MINIO_REGIONS_TABLE)
        # return self._write_to_minio(parsed, "regions")

    def consume_categories(self):
        schema_str = self.schema_provider.struct_to_schema_string(self.schema_provider.category_schema(), multiline=True)
        self.create_table(schema_str, MINIO_YOUTUBE_TRENDS_CATALOG, MINIO_BRONZE_NAMESPACE, MINIO_CATEGORIES_TABLE)
        
        df = self._read_kafka(KAFKA_TOPIC_CATEGORIES)
        parsed = self._parse_json(df, self.schema_provider.category_schema())
        return self._write_to_iceberg(parsed, MINIO_BRONZE_NAMESPACE,  MINIO_CATEGORIES_TABLE)
        # return self._write_to_minio(parsed, "categories")

    def consume_videos(self):
        schema_str = self.schema_provider.struct_to_schema_string(self.schema_provider.videos_schema(), multiline=True)
        self.create_table(schema_str, MINIO_YOUTUBE_TRENDS_CATALOG, MINIO_BRONZE_NAMESPACE, MINIO_VIDEOS_TABLE)
        
        df = self._read_kafka(KAFKA_TOPIC_VIDEOS)        
        parsed = self._parse_json(df, self.schema_provider.videos_schema())
        
        return self._write_to_iceberg(parsed, MINIO_BRONZE_NAMESPACE,  MINIO_VIDEOS_TABLE)
        # return self._write_to_minio(parsed, "videos")

    def consume_comments(self):
        df = self._read_kafka(KAFKA_TOPIC_COMMENTS)
        parsed = self._parse_json(df, self.schema_provider.comments_schema())
        return self._write_to_minio(parsed, "comments")

    def create_namespace(self, catalog, namespace):                                           
        self.spark.sql(f"""
            CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}             
            LOCATION 's3a://{MINIO_YOUTUBE_TRENDS_BUCKET}/{namespace}'
            """)

        print(f"✓ Created namespace {catalog}.{namespace}")


    def create_table(self, schema_str, catalog, namespace, table):                                           
        
        self.create_namespace(catalog, namespace)
        print(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.{table} (                        
            {schema_str}
        )
        USING iceberg
        LOCATION 's3a://{MINIO_YOUTUBE_TRENDS_BUCKET}/{namespace}/{table}'
        """
        )

        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.{table} (                        
            {schema_str}
        )
        USING iceberg
        LOCATION 's3a://{MINIO_YOUTUBE_TRENDS_BUCKET}/{namespace}/{table}'
        """)

        print(f"✓ Created table {catalog}.{namespace}.{table}")    