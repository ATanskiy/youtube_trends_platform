import argparse
from orchestrator import Orchestrator
# Set the display width to a specific number of characters

def main(name):
    orchestrator = Orchestrator()
    # regions = orchestrator.get_regions_list()
    # print(regions)    
    regions = ["US", "IL"]     

    if name == "regions":
        orchestrator.produce_regions() 
    if name == "categories":
        orchestrator.produce_categories(["US"])    
    if name == "videos":
        orchestrator.produce_videos(regions)            
    
    # Produce data
    # orchestrator.produce_regions_df()
    # orchestrator.produce_categories_df(["US"])
    # orchestrator.produce_videos_df(regions)    
    # orchestrator.produce_comments(['VIDEO_ID1', 'VIDEO_ID2'])    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", type=str, help="Job name to run")    
    args = parser.parse_args()
    main(args.name)
