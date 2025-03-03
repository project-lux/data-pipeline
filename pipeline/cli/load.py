
from pipeline.process.load_manager import LoadManager

def handle_command(cfgs, args, rest):
    wks = args.max_workers
    lm = LoadManager(cfgs, wks)
    lm.prepare_single("ulan")
    lm.load_all()


