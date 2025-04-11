
from ..process.load_manager import LoadManager
from argparse import ArgumentParser
from ._rich import Live, get_layout

def handle_command(cfgs, args, rest):
    wks = args.max_workers if args.max_workers > 0 else cfgs.max_workers

    ap = ArgumentParser()
    ap.add_argument("--cache", type=str, default="recordcache", help="Type of cache to export")
    ap.add_argument('--type', type=str, default="marklogic", help="Which type of export to generate")
    ap.parse_args(rest, namespace=args)

    if not args.source:
        args.source = "merged"
    elif args.source == "all":
        args.source = "merged"
    sources = args.source.split(',')

    xm = ExportManager(cfgs, wks)
    xm.export_type = args.type
    xm.cache = args.cache

    for s in sources:
        xm.prepare_single(s)

    # Here we set up the rich UI
    if args.no_ui:
        xm.process(None, disable_ui=args.no_ui, export_type=args.type)
    else:
        layout = get_layout(cfgs, wks, args.log)
        with Live(layout, screen=False, refresh_per_second=4) as live:
            # And calling this will manage the multiprocessing
            xm.process(layout, disable_ui=args.no_ui, export_type=args.type)
