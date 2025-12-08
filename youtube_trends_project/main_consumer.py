from orchestrator import Orchestrator
# Set the display width to a specific number of characters

def main():
    orchestrator = Orchestrator()
    # regions = orchestrator.get_regions_list()
    # print(regions)    
    regions = ["IL"]     

    # Start Spark streaming
    orchestrator.start_spark_streams()

if __name__ == "__main__":
    main()
