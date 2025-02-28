
from pipeline.process.download_manager import DownloadManager

def handle_command(cfgs, args, rest):
    manager = DownloadManager(cfgs)

    if not args.source:
        args.source = "all"

    if args.source == "all":
        manager.prepare_all()
    else:
        if args.source in ['internal', 'external']:
            sources = list(getattr(cfgs, args.source).keys())
        else:
            sources = args.source.split(',')
        for s in sources:
            manager.prepare_single(s)

    okay = manager.download_all(disable_tqdm=args.no_tqdm, verbose=args.verbose)
    if not okay:
        # at least one failed
        print(manager.downloads)
