
from argparse import ArgumentParser
from ._rich import Live, get_layout

class BaseHandler:
    def __init__(self, cfgs):
        self.configs = cfgs
        self.default_source = "all"

    def add_args(self, ap):
        return None

    def process(self, args, rest):
        if rest:
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
        elif args.source in ['internal', 'external']:
            sources = list(getattr(self.configs, args.source).keys())
        else:
            sources = args.source.split(',')

        manager = self.make_manager(wks, args)
        for s in sources:
            manager.prepare_single(s)

        xargs = {x:getattr(args, y) for x,y in self.extra_args.items()}

        # Here we set up the rich UI
        if args.no_ui:
            layout = None
            manager.process(layout, engine=args.engine, disable_ui=args.no_ui, **xargs)
        else:
            layout = get_layout(self.configs, wks, args.log)
            with Live(layout, screen=False, refresh_per_second=4) as live:
                # And calling this will manage the multiprocessing
                manager.process(layout, engine=args.engine, disable_ui=args.no_ui, **xargs)
