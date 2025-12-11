import sys
import argparse
from orchestrator import Orchestrator
# Set the display width to a specific number of characters

def main(name):
    orchestrator = Orchestrator()
    
    # Start Spark streaming
    orchestrator.start_spark_streams(name)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", type=str, help="Job name to run")    
    args = parser.parse_args()
    main(args.name)
