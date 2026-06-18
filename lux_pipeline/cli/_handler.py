import os
from argparse import ArgumentParser
from datetime import datetime
from ._rich import Live, get_layout


class BaseHandler:
    def __init__(self, cfgs):
        self.configs = cfgs
        self.default_source = "all"

    def add_args(self, ap):
        return None

    def process(self, args, rest):
        # can't rely on rest being true, as could have extra args with defaults
        ap = ArgumentParser()
        self.add_args(ap)
        ap.parse_args(rest, namespace=args)


class CommandHandler(BaseHandler):
    def __init__(self, cfgs):
        super().__init__(cfgs)
        self.extra_args = {}

    def make_manager(self, workers, args):
        raise NotImplementedError()

    def process(self, args, rest):
        super().process(args, rest)

        wks = args.max_workers if args.max_workers > 0 else self.configs.max_workers
        if not args.source:
            args.source = self.default_source
        if args.source == "all":
            sources = [*self.configs.internal.keys(), *self.configs.external.keys()]
        elif args.source in ["internal", "external"]:
            sources = list(getattr(self.configs, args.source).keys())
        else:
            sources = args.source.split(",")

        xargs = {x: getattr(args, y) for x, y in self.extra_args.items()}

        manager = self.make_manager(wks, args)
        if getattr(args, "log", None):
            manager.log_level = args.log
        # Per-worker log files are the log of record; the UI only displays them
        log_dir = getattr(self.configs, "log_dir", None) or "."
        os.makedirs(log_dir, exist_ok=True)
        started = datetime.now().strftime("%Y-%m-%d-%H%M%S")
        command = getattr(args, "command", None) or "task"
        manager.log_file_prefix = os.path.join(log_dir, f"{command}-{started}")
        for s in sources:
            manager.prepare_single(s)

        # Here we set up the rich UI
        if args.no_ui:
            layout = None
            print(xargs)
            manager.process(layout, engine=args.engine, disable_ui=args.no_ui, **xargs)
        else:
            layout = get_layout(self.configs, wks, args.log)
            with Live(layout, screen=False, refresh_per_second=4):
                # And calling this will manage the multiprocessing
                manager.process(layout, engine=args.engine, disable_ui=args.no_ui, **xargs)
