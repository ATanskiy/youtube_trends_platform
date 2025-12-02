from orchestrator import Orchestrator

def main():
    orchestrator = Orchestrator()

    # Produce data
    orchestrator.produce_regions_df()
    # orchestrator.produce_categories()
    # orchestrator.produce_videos()
    # orchestrator.produce_comments(['VIDEO_ID1', 'VIDEO_ID2'])

    # Start Spark streaming
    # orchestrator.start_spark_streams()

if __name__ == "__main__":
    main()
