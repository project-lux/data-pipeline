
from pipeline.process.load_manager import LoadManager

def handle_command(cfgs, args, rest):
    print(f"Got load command with args: {args}")
    wks = args.max_workers

    lm = LoadManager(cfgs, wks)
    lm.prepare_single("aat")
    lm.load_all()


