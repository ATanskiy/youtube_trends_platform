import argparse

# from youtube_trends.app_container import AppContainer
from youtube_trends.app_container  import AppContainer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="YouTube Trends Kafka producers")
    parser.add_argument("--region", help="Region code (e.g. IL, US)", default="IL")
    parser.add_argument("--fetch-comments-for-video", help="Video ID to fetch comments for")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    container = AppContainer()

    # Ingest regions (typically once)
    container.region_ingestion.ingest_all()

    # Ingest categories and trending videos for region
    container.category_ingestion.ingest_for_region(args.region)
    container.video_ingestion.ingest_for_region(args.region)

    # Optionally ingest comments for a specific video
    if args.fetch_comments_for_video:
        container.comment_ingestion.ingest_for_video(args.fetch_comments_for_video)


if __name__ == "__main__":
    main()
