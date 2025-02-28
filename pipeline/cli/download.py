
from pipeline.process.download_manager import DownloadManager

def handle_command(cfgs, args, rest):
    manager = DownloadManager(cfgs)
    sources = args.source.split(',')
    if sources == ['all']:
        manager.prepare_all()
    else:
        for s in sources:
            manager.prepare_single(s)
    okay = manager.download_all(disable_tqdm=args.no_tqdm, verbose=args.verbose)
    if not okay:
        # at least one failed
        print(manager.downloads)
