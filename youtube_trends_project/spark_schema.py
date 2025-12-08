from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class SparkSchema:
    @staticmethod
    def region_schema() -> StructType:
        return StructType([
            StructField("etag", StringType(), True),
            StructField("id", StringType(), True),
            StructField("snippet", StructType([
                StructField("gl", StringType(), True),
                StructField("name", StringType(), True)
            ]))
        ])

    @staticmethod
    def category_schema() -> StructType:
        return StructType([
            StructField("etag", StringType(), True),
            StructField("id", StringType(), True),
            StructField("snippet", StructType([
                StructField("title", StringType(), True),
                StructField("assignable", StringType(), True),
                StructField("channelId", StringType(), True)
            ]))
        ])

    @staticmethod
    def videos_schema() -> StructType:
        return StructType([
            StructField("etag", StringType(), True),
            StructField("id", StringType(), True),
            StructField("snippet", StructType([
                StructField("publishedAt", StringType(), True),
                StructField("channelId", StringType(), True),
                StructField("title", StringType(), True),
                StructField("description", StringType(), True),
                StructField("categoryId", StringType(), True)
            ])),
            StructField("statistics", StructType([
                StructField("viewCount", StringType(), True),
                StructField("likeCount", StringType(), True),
                StructField("commentCount", StringType(), True)
            ]))
        ])

    @staticmethod
    def comments_schema() -> StructType:
        return StructType([
            StructField("etag", StringType(), True),
            StructField("id", StringType(), True),
            StructField("snippet", StructType([
                StructField("videoId", StringType(), True),
                StructField("textDisplay", StringType(), True),
                StructField("authorDisplayName", StringType(), True),
                StructField("likeCount", IntegerType(), True),
                StructField("publishedAt", StringType(), True)
            ]))
        ])
