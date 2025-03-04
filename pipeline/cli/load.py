
from pipeline.process.load_manager import LoadManager
from argparse import ArgumentParser

def handle_command(cfgs, args, rest):
    wks = args.max_workers

    if rest:
        ap = ArgumentParser()
        ap.add_argument('--type', type=str, default="record")
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
    lm.load_all(disable_tqdm=args.no_tqdm, verbose=args.verbose, overwrite=args.no_overwrite, which=args.type)
