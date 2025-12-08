#!/bin/sh

echo "Starting MinIO bucket initialization..."

# Wait until MinIO is ready
until mc alias set minio_server http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"; do
    echo "Waiting for MinIO..."
    sleep 3
done

sleep 2

create_bucket() {
    BUCKET=$1

    # Try listing the bucket. If exit code == 0, it exists.
    if mc ls "minio_server/$BUCKET" > /dev/null 2>&1; then
        echo "Bucket '$BUCKET' already exists."
    else
        echo "Bucket '$BUCKET' does not exist. Creating..."
        mc mb "minio_server/$BUCKET"
        echo "Bucket '$BUCKET' created!"
    fi
}

# Check or create buckets
create_bucket default
create_bucket hive
create_bucket youtube-trends

echo "Bucket initialization completed."
