
from pipeline.process.download_manager import DownloadManager
from argparse import ArgumentParser

def handle_command(cfgs, args, rest):
    manager = DownloadManager(cfgs)

    if not args.source:
        args.source = "all"

    ap = ArgumentParser()
    ap.add_argument('--type', type=str, default="record")
    ap.add_argument("--no-overwrite", action='store_false', help="Do not overwrite existing files/records")
    ap.parse_args(rest, namespace=args)

    if args.source == "all":
        manager.prepare_all()
    else:
        if args.source in ['internal', 'external']:
            sources = list(getattr(cfgs, args.source).keys())
        else:
            sources = args.source.split(',')
        for s in sources:
            manager.prepare_single(s)

    if not manager.downloads:
        print("Could not find anything to download!")
    else:
        okay = manager.download_all(disable_ui=args.no_ui, verbose=args.verbose)
        if not okay:
            # at least one failed
            print(manager.downloads)
