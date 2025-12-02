import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# ----------------------------
# YouTube API
# ----------------------------
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

# ----------------------------
# Kafka
# ----------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC_REGIONS = os.getenv("KAFKA_TOPIC_REGIONS", "youtube_regions")
KAFKA_TOPIC_CATEGORIES = os.getenv("KAFKA_TOPIC_CATEGORIES", "youtube_categories")
KAFKA_TOPIC_VIDEOS = os.getenv("KAFKA_TOPIC_VIDEOS", "youtube_videos")
KAFKA_TOPIC_COMMENTS = os.getenv("KAFKA_TOPIC_COMMENTS", "youtube_comments")

# ----------------------------
# MinIO / S3
# ----------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://127.0.0.1:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_PREFIX = os.getenv("MINIO_BUCKET_PREFIX", "youtube")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
