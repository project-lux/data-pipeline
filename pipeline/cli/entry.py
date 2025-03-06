import os
import sys
from argparse import ArgumentParser
from dotenv import load_dotenv, find_dotenv
import importlib
from pipeline.config import Config

fn = find_dotenv(usecwd=True)
if fn:
    load_dotenv(fn)
basepath = os.getenv("LUX_BASEPATH", "")
idmap = None
if not basepath:
    cfgs = None
else:
    try:
        cfgs = Config(basepath=basepath)
        idmap = cfgs.get_idmap()
        cfgs.cache_globals()
        cfgs.instantiate_all()
    except:
        cfgs = None

def handle_command(cfgs, args, rest):
    print("There isn't an 'entry' command, please see 'lux help' for the list")
    return False

def main():

    parser = ArgumentParser()
    parser.add_argument("-i", action="store_true", help="If provided, drop to interactive console after the command")
    parser.add_argument("command", type=str, help="Function to execute, see 'lux help' for the list")
    parser.add_argument("--source", type=str, help="Source(s) to download separated by commas, or 'all'")
    parser.add_argument("--max_workers", type=int, default=0, help="Number of processes to use")
    parser.add_argument("--no-ui", action='store_true', help="If set, then disable the user interface")
    parser.add_argument("--verbose", type=str, help="Enable verbose output")

    args, rest = parser.parse_known_args()

    if cfgs is None and args.command not in ['initialize', 'testinstall']:
        print("Please use 'lux initialize <base directory>' first to create your installation or lux testinstall to diagnose issues")
        sys.exit(0)
    elif args.command == 'initialize' and cfgs is not None:
        print(f"You have already initialized your LUX pipeline. The configs are at: {basepath}")
        sys.exit(0)

    try:
        mod = importlib.import_module(f'pipeline.cli.{args.command}')
    except Exception as e:
        print(f"Failed to import command {args.command}:\n{e}")
        sys.exit(0)

    try:
        result = mod.handle_command(cfgs, args, rest)
    except Exception as e:
        print(f"Failed to process command: {args}\n{e}")

    if args.i and args.command != "interactive":
        mod = importlib.import_module(f'pipeline.cli.interactive')
        mod.handle_command(cfgs, args, rest)

if __name__ == "__main__":
    main()
