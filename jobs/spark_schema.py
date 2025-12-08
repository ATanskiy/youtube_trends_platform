from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class SparkSchema:
    @staticmethod
    def region_schema() -> StructType:
        return StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),            
        ])

    @staticmethod
    def category_schema() -> StructType:
        return StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),            
        ])

    @staticmethod
    def videos_schema() -> StructType:
        return StructType([
            StructField("id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("publishedAt", StringType(), True),
            StructField("channelId", StringType(), True),
            StructField("channelTitle", StringType(), True),
            StructField("categoryId", StringType(), True),
            StructField("duration", StringType(), True),
            StructField("viewCount", IntegerType(), True),
            StructField("likeCount", IntegerType(), True),
            StructField("favoriteCount", IntegerType(), True),
            StructField("commentCount", IntegerType(), True),
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
