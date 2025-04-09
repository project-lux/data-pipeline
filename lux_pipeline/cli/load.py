
from ..process.load_manager import LoadManager
from argparse import ArgumentParser
from ._rich import Live, get_layout

def handle_command(cfgs, args, rest):
    wks = args.max_workers if args.max_workers > 0 else cfgs.max_workers

    ap = ArgumentParser()
    ap.add_argument('--type', type=str, default="records")
    ap.add_argument("--no-overwrite", action='store_false', help="Do not overwrite existing files/records")
    ap.parse_args(rest, namespace=args)

    if not args.source:
        args.source = "all"
    if args.source == "all":
        sources = [*cfgs.internal.keys(), *cfgs.external.keys()]
    elif args.source in ['internal', 'external']:
        sources = list(getattr(cfgs, args.source).keys())
    else:
        sources = args.source.split(',')

    lm = LoadManager(cfgs, wks)
    for s in sources:
        lm.prepare_single(s)

    # Here we set up the rich UI
    if args.no_ui:
        layout = None
        lm.process(layout, disable_ui=args.no_ui, overwrite=args.no_overwrite, load_type=args.type)
    else:
        layout = get_layout(cfgs, wks, args.log)
        with Live(layout, screen=False, refresh_per_second=4) as live:
            # And calling this will manage the multiprocessing
            lm.process(layout, disable_ui=args.no_ui, overwrite=args.no_overwrite, load_type=args.type)
