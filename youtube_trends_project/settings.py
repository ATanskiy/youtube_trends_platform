import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# ----------------------------
# YouTube API
# ----------------------------
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
YOUTUBE_BASE_URL= os.getenv("YOUTUBE_BASE_URL")

# ----------------------------
# Kafka
# ----------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC_REGIONS = os.getenv("KAFKA_TOPIC_REGIONS")
KAFKA_TOPIC_CATEGORIES = os.getenv("KAFKA_TOPIC_CATEGORIES")
KAFKA_TOPIC_VIDEOS = os.getenv("KAFKA_TOPIC_VIDEOS")
KAFKA_TOPIC_COMMENTS = os.getenv("KAFKA_TOPIC_COMMENTS")

# ----------------------------
# MinIO / S3
# ----------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_URL")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET_PREFIX = os.getenv("S3_PREFIX")
AWS_REGION = os.getenv("AWS_REGION")
