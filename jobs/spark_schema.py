from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType

class SparkSchema:
    @staticmethod
    def region_schema() -> StructType:
        return StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),            
            StructField("created_at", StringType(), True)
        ])

    @staticmethod
    def language_schema() -> StructType:
        return StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),            
            StructField("created_at", StringType(), True)
        ])
    
    @staticmethod
    def category_schema() -> StructType:
        return StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("created_at", StringType(), True)
        ])

    @staticmethod
    def videos_schema() -> StructType:
        return StructType([
            StructField("id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("region_id", StringType(), True),
            StructField("language_id", StringType(), True),
            StructField("language_id_src", StringType(), True),
            StructField("published_at", StringType(), True),            
            StructField("channel_id", StringType(), True),
            StructField("channel_title", StringType(), True),
            StructField("category_id", IntegerType(), True),
            StructField("duration", StringType(), True),
            StructField("view_count", IntegerType(), True),
            StructField("like_count", IntegerType(), True),
            StructField("favorite_count", IntegerType(), True),
            StructField("comment_count", IntegerType(), True),
            StructField("created_at", StringType(), True)
        ])

    @staticmethod
    def comments_schema() -> StructType:
        return StructType([            
            StructField("id", StringType(), True),            
            StructField("videoId", StringType(), True),
            StructField("textDisplay", StringType(), True),
            StructField("authorDisplayName", StringType(), True),
            StructField("likeCount", IntegerType(), True),
            StructField("publishedAt", StringType(), True)
        ])
    
    @staticmethod
    def struct_to_schema_string(schema: StructType, multiline=False):
        lines = []

        for field in schema.fields:
            name = field.name
            dtype = field.dataType.simpleString().upper()  # convert spark types to uppercase
            nullable = field.nullable

            # Optional: include NULL/NOT NULL
            # null_str = "NULL" if nullable else "NOT NULL"

            lines.append(f"{name} {dtype}")

        return ",\n".join(lines) if multiline else ", ".join(lines)
