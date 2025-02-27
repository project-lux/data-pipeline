import os
import sys
from argparse import ArgumentParser
from dotenv import load_dotenv
import importlib
from pipeline.config import Config

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

def main():
    parser = ArgumentParser()
    parser.add_argument("command", type=str, help="Function to execute, see 'lux help' for the list")
    parser.add_argument("--source", type=str, help="Source(s) to download separated by commas, or 'all'")
    parser.add_argument("--verbose", type=str, help="Enable verbose output")
    parser.add_argument("--max_workers", type=int, default=0, help="Number of processes to use")
    parser.add_argument("--cache", type=str, help="Types of cache separated by commas, or 'all'")
    args = parser.parse_args()

    try:
        mod = importlib.import_module(f'pipeline.cli.{args.command}')
    except Exception as e:
        print(f"Failed to import command {args.command}:\n{e}")
        sys.exit(0)

    try:
        result = mod.handle_command(cfgs, args)
    except Exception as e:
        print(f"Failed to process command: {args}\n{e}")

if __name__ == "__main__":
    main()
