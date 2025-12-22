# jobs/load_videos_csv_to_bronze.py

from spark_session import SparkSessionFactory
from spark_schema import SparkSchema

CSV_PATH = "/opt/streaming/jobs/videos_202512171748.csv"
TARGET_TABLE = "youtube_trends.bronze.videos"

def main():
    spark = SparkSessionFactory().create_session(
        app_name="csv_to_bronze_videos"
    )

    df = (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("quote", "\"")
        .option("escape", "\"")
        .schema(SparkSchema.videos_schema())
        .csv(CSV_PATH)
    )

    # 🔍 optional preview
    df.show(5, truncate=False)
    print(f"Rows: {df.count()}")

    # ✅ Schema matches Iceberg exactly — safe to write
    df.writeTo(TARGET_TABLE).append()

    print("✅ CSV data written to Iceberg Bronze videos table")

if __name__ == "__main__":
    main()