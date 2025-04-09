from argparse import ArgumentParser
from ..process.reconcile_manager import ReconcileManager
from ._rich import Live, get_layout

def handle_command(cfgs, args, rest):
    wks = args.max_workers

    ap = ArgumentParser()
    ap.add_argument('--recid', type=str, help="Comma separated list of recids")
    ap.add_argument('--no-refs', action="store_true", help="Should not process references")
    ap.parse_args(rest, namespace=args)

    if not args.source:
        args.source = "all"
    if args.source == "all":
        sources = [*cfgs.internal.keys(), *cfgs.external.keys()]
    elif args.source in ['internal', 'external']:
        sources = list(getattr(cfgs, args.source).keys())
    else:
        sources = args.source.split(',')

    rm = ReconcileManager(cfgs, wks)
    for s in sources:
        rm.prepare_single(s)

    if args.no_ui:
        layout = None
        rm.process(layout, engine=args.engine, disable_ui=args.no_ui, no_refs=args.no_refs)
    else:
        layout = get_layout(cfgs, wks, args.log)

        with Live(layout, screen=False, refresh_per_second=4) as live:
            # And calling this will manage the multiprocessing
            rm.process(layout, engine=args.engine, disable_ui=args.no_ui, no_refs=args.no_refs)
