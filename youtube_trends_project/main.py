import argparse
from orchestrator import Orchestrator
# Set the display width to a specific number of characters

def main(name):
        
    regions = ["US", "IL"]     
    # Produce data
    orchestrator = Orchestrator().start(name, regions)    
                                        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", type=str, help="Job name to run")    
    args = parser.parse_args()
    main(args.name)
