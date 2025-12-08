from orchestrator import Orchestrator
# Set the display width to a specific number of characters

def main():
    orchestrator = Orchestrator()
    # regions = orchestrator.get_regions_list()
    # print(regions)    
    regions = ["US", "IL"]     

    # Produce data
    # orchestrator.produce_regions_df()
    # orchestrator.produce_categories_df(["US"])
    # orchestrator.produce_videos_df(regions)    
    # orchestrator.produce_comments(['VIDEO_ID1', 'VIDEO_ID2'])

    orchestrator.produce_regions() 
    orchestrator.produce_categories(["US"])
    orchestrator.produce_videos(regions)        

if __name__ == "__main__":
    main()