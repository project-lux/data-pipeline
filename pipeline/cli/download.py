
from pipeline.process.download_manager import DownloadManager

def handle_command(cfgs, args):
    manager = DownloadManager(cfgs)
    sources = args.source.split(',')
    if sources == ['all']:
        manager.prepare_all()
    else:
        for s in sources:
            manager.prepare_single(s)
    manager.download_all(verbose=args.verbose)
