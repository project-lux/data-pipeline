
from lux_pipeline.process.download_manager import DownloadManager
from argparse import ArgumentParser
from ._rich import Live, get_layout

def handle_command(cfgs, args, rest):
    wks = args.max_workers if args.max_workers > 0 else cfgs.max_workers
    manager = DownloadManager(cfgs, max_workers=wks)

    if not args.source:
        args.source = "all"

    ap = ArgumentParser()
    ap.add_argument('--type', type=str, default="all", help="all|records|export for which type to download")
    ap.add_argument("--no-overwrite", action='store_false', help="Do not overwrite existing files/records")
    ap.parse_args(rest, namespace=args)

    manager.download_type = args.type
    if args.source == "all":
        manager.prepare_all()
    else:
        if args.source in ['internal', 'external']:
            sources = list(getattr(cfgs, args.source).keys())
        else:
            sources = args.source.split(',')
        for s in sources:
            manager.prepare_single(s)

    if not manager.sources:
        print("Could not find anything to download!")
    else:
        if args.no_ui:
            layout = None
            manager.process(layout, disable_ui=args.no_ui, verbose=args.verbose)
        else:
            layout = get_layout(cfgs, wks)
            with Live(layout, screen=True, refresh_per_second=4) as live:
                # And calling this will manage the multiprocessing
                manager.process(layout, disable_ui=args.no_ui, verbose=args.verbose)
