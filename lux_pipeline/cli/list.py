from ._handler import BaseHandler as BH


class CommandHandler(BH):
    def process(self, args, rest):
        super().process(args, rest)
        cfgs = self.configs

        if not args.source or args.source == "all":
            sources = [
                *cfgs.internal.keys(),
                *cfgs.external.keys(),
                *cfgs.results.keys(),
            ]
        elif args.source in ["internal", "external", "results"]:
            sources = list(getattr(cfgs, args.source).keys())
        else:
            sources = args.source.split(",")

        for s in sources:
            if s in cfgs.internal:
                cfg = cfgs.internal[s]
                which = "internal"
            elif s in cfgs.external:
                cfg = cfgs.external[s]
                which = "external"
            elif s in cfgs.results:
                cfg = cfgs.results[s]
                which = "results"
            else:
                # uhh...
                print(f"Could not find source: {s}")
                continue
            print(f"Source: {cfg['name']} ({which})")
