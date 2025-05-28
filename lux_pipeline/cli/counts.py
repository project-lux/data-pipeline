from ._handler import BaseHandler as BH


class CommandHandler(BH):
    def add_args(self, ap):
        ap.add_argument("--cache", type=str, help="Types of cache separated by commas, or 'all'")
        ap.add_argument("--map", type=str, help="Types of map separated by commas, or 'all'")
        ap.add_argument("--index", type=str, help="Index sources separated by commas, or 'all'")

    def process(self, args, rest):
        super().process(args, rest)
        cfgs = self.configs

        if not hasattr(args, "map") or not args.map:
            maps = []
        elif args.map == "all":
            maps = list(cfgs.map_stores.keys())
        else:
            maps = args.map.split(",")

        indexes = []
        if not hasattr(args, "index") or not args.index:
            pass
        elif args.index == "all":
            # need to walk and look for them :/
            for cfg_list in [cfgs.internal, cfgs.external, cfgs.results]:
                for cfg in cfg_list.values():
                    if "indexes" in cfg:
                        for idx in cfg["indexes"].values():
                            idx["source"] = cfg["name"]
                            indexes.append(idx)
        else:
            idx_srcs = args.index.split(",")
            for s in idx_srcs:
                if s in cfgs.internal:
                    cfg = cfgs.internal[s]
                elif s in cfgs.external:
                    cfg = cfgs.external[s]
                elif s in cfgs.results:
                    cfg = cfgs.results[s]
                else:
                    raise ValueError(f"Unknown index source: {s}")
                if "indexes" in cfg:
                    for idx in cfg["indexes"].values():
                        idx["source"] = cfg["name"]
                        indexes.append(idx)

        if not args.source and (maps or indexes):
            sources = []
        elif not args.source or args.source == "all":
            sources = [*cfgs.internal.keys(), *cfgs.external.keys(), *cfgs.results.keys()]
        elif args.source in ["internal", "external", "results"]:
            sources = list(getattr(cfgs, args.source).keys())
        else:
            sources = args.source.split(",")

        if not hasattr(args, "cache") or not args.cache:
            caches = ["datacache", "recordcache", "reconciledRecordcache", "recordcache2"]
        else:
            caches = args.cache.split(",")

        total = 0
        for s in sources:
            if s in cfgs.internal:
                cfg = cfgs.internal[s]
            elif s in cfgs.external:
                cfg = cfgs.external[s]
            elif s in cfgs.results:
                cfg = cfgs.results[s]
            else:
                # uhh...
                print(f"Could not find cache to count: {s}")
                continue

            for c in caches:
                cache = cfg.get(c, None)
                if cache is not None:
                    est = cache.len_estimate()
                    pref = "~"
                    if est < 100000:
                        est = len(cache)
                        pref = "="
                    ttl = f"{s.rjust(16)}/{c}:".ljust(36)
                    print(f"{ttl} {pref}{est}")
                    total += est
        if sources:
            ttl = "Total Cache Records Seen:".rjust(36)
            print(f"{ttl} ~{total}")

        if maps:
            for m in maps:
                if m in cfgs.map_stores:
                    cfg = cfgs.map_stores[m]
                    store = cfg.get("store", None)
                    if store is not None:
                        n = len(store)
                    else:
                        # force it to be instantiated
                        store = cfgs.instantiate_map(m)["store"]
                        n = len(store)
                    total += n
                    ttl = f"{m.rjust(16)}:".ljust(36)
                    print(f"{ttl} {n}")
            ttl = "Total Map Records Seen:".rjust(36)
            print(f"{ttl} ~{total}")

        if indexes:
            for idx in indexes:
                idx["index"].open()
                n = len(idx["index"])
                total += n
                ttl = f"{idx['source'].rjust(16)}/{idx['name']}:".ljust(36)
                print(f"{ttl} {n}")
            ttl = "Total Index Records Seen:".rjust(36)
            print(f"{ttl} ~{total}")
