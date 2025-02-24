import os
import sys
from argparse import ArgumentParser
from dotenv import load_dotenv

sys.path = [os.getcwd(), *sys.path]

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
    parser.add_argument("--source", type=str, help="Source(s) to download separated by commas, or 'all'", required=True)
    parser.add_argument("--verbose", type=str, help="Enable verbose output")
    parser.add_argument("--max_workers", type=int, default=0, help="Number of processes to use")
    args = parser.parse_args()

    if args.command == "download":
        from pipeline.process.download_manager import DownloadManager
        manager = DownloadManager(cfgs)
        sources = args.source.split(',')
        if sources == ['all']:
            manager.prepare_all()
        else:
            for s in sources:
                manager.prepare_single(s)
        manager.download_all(verbose=args.verbose)

if __name__ == "__main__":
    main()
