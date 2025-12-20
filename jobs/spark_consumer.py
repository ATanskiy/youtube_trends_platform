import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_schema import SparkSchema
from settings import (
    KAFKA_TOPIC_REGIONS,
    KAFKA_TOPIC_LANGUAGES,
    KAFKA_TOPIC_CATEGORIES,
    KAFKA_TOPIC_VIDEOS,
    KAFKA_TOPIC_COMMENTS,
    KAFKA_BOOTSTRAP_SERVERS,
    MINIO_YOUTUBE_TRENDS_BUCKET,
    MINIO_YOUTUBE_TRENDS_CATALOG,
    MINIO_BRONZE_NAMESPACE,
    MINIO_REGIONS_TABLE,
    MINIO_LANGUAGES_TABLE,
    MINIO_CATEGORIES_TABLE,
    MINIO_VIDEOS_TABLE
)


class SparkKafkaConsumer:
    def __init__(self, spark: SparkSession, schema_provider: SparkSchema):
        self.spark = spark
        self.schema_provider = schema_provider

    def _read_batch_kafka(self, topic):
        return (
            self.spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", topic)                    
            .load()
        )        

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

    def deduplicate_source_before_merge(self, df):
        window = Window.partitionBy("id").orderBy(F.col("created_at").desc())

        dedup_df = (
            df
            .withColumn("rn", F.row_number().over(window))
            .filter(F.col("rn") == 1)
            .drop("rn")
        )   
        return dedup_df
            

    def write_to_iceberg(self, df, namespace, table, pk="id"):
        try:
            row_count = df.count()
            print("=" * 80)
            print(f"🔥 UPSERTING → Iceberg table: {MINIO_YOUTUBE_TRENDS_CATALOG}.{namespace}.{table}")
            print(f"Rows: {row_count}")
            print("=" * 80)

            if row_count == 0:
                return
            # Get all columns except primary key for the UPDATE SET clause
            columns = [col for col in df.columns if col != pk]
            update_set_clause = ", ".join([f"t.{col} = s.{col}" for col in columns])

            # Register temp view
            temp_view = f"{table}_updates"
            df.createOrReplaceTempView(temp_view)

            # Dynamic MERGE statement for upsert
            merge_sql = f"""
            MERGE INTO {MINIO_YOUTUBE_TRENDS_CATALOG}.{namespace}.{table} t
            USING {temp_view} s
            ON t.{pk} = s.{pk}
            WHEN MATCHED THEN
                UPDATE SET {update_set_clause}
            WHEN NOT MATCHED THEN
                INSERT *
            """
            df.sparkSession.sql(merge_sql)
            print(f"✅ Successfully upserted {row_count} rows")

        except Exception:
            print(f"❌ ERROR in write_{namespace}.{table}_to_iceberg()")
            traceback.print_exc()
    
    def write_videos_to_iceberg(self, df, batch_id):
        try:
            row_count = df.count()
            print("=" * 80)
            print(f"🔥 Writing batch {batch_id} → Iceberg table: {MINIO_YOUTUBE_TRENDS_CATALOG}.{MINIO_BRONZE_NAMESPACE}.{MINIO_VIDEOS_TABLE}")
            print(f"Rows: {row_count}")
            print("=" * 80)

            if row_count > 0:
                df.writeTo(f"{MINIO_YOUTUBE_TRENDS_CATALOG}.{MINIO_BRONZE_NAMESPACE}.{MINIO_VIDEOS_TABLE}").append()                
        except Exception:
            print("❌ ERROR in write_to_iceberg()")
            traceback.print_exc()
    

    def _write_to_iceberg(self, df, namespace, table):                
        return (
        df.writeStream.foreachBatch(self.write_videos_to_iceberg)
        .outputMode("append")
        .option("checkpointLocation", f"s3a://{MINIO_YOUTUBE_TRENDS_BUCKET}/checkpoints/{namespace}/{table}")
        .option("startingOffsets", "earliest")                
        .trigger(processingTime="10 seconds")
        )

    def consume_regions(self):
        df = self._read_batch_kafka(KAFKA_TOPIC_REGIONS)        
        parsed = self._parse_json(df, self.schema_provider.region_schema())
        dedup = self.deduplicate_source_before_merge(parsed)
        self.write_to_iceberg(dedup, MINIO_BRONZE_NAMESPACE, MINIO_REGIONS_TABLE)        

    def consume_languages(self):
        df = self._read_batch_kafka(KAFKA_TOPIC_LANGUAGES)        
        parsed = self._parse_json(df, self.schema_provider.region_schema())
        dedup = self.deduplicate_source_before_merge(parsed)
        self.write_to_iceberg(dedup, MINIO_BRONZE_NAMESPACE, MINIO_LANGUAGES_TABLE)

    def consume_categories(self):    
        df = self._read_batch_kafka(KAFKA_TOPIC_CATEGORIES)
        parsed = self._parse_json(df, self.schema_provider.category_schema())
        dedup = self.deduplicate_source_before_merge(parsed)        
        self.write_to_iceberg(dedup, MINIO_BRONZE_NAMESPACE,  MINIO_CATEGORIES_TABLE)

    def consume_videos(self):
        df = self._read_kafka(KAFKA_TOPIC_VIDEOS)        
        parsed = self._parse_json(df, self.schema_provider.videos_schema())        
        return self._write_to_iceberg(parsed, MINIO_BRONZE_NAMESPACE,  MINIO_VIDEOS_TABLE)

    def consume_comments(self):
        df = self._read_kafka(KAFKA_TOPIC_COMMENTS)
        parsed = self._parse_json(df, self.schema_provider.comments_schema())
        return self._write_to_iceberg(parsed, "comments")