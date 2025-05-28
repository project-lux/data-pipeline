from ._handler import BaseHandler as BH


class CommandHandler(BH):
    def add_args(self, ap):
        ap.add_argument("--cache", type=str, help="Types of cache separated by commas, or 'all'")
        ap.add_argument("--map", type=str, help="Name of map to clear, no comma separation")
        ap.add_argument("--index", type=str, help="Index sources separated by commas, or 'all'")
        ap.add_argument("--force", action="store_true")

    def process(self, args, rest):
        super().process(args, rest)
        cfgs = self.configs

        if args.map:
            # Get the map from config
            map = args.map.strip()
            if not map:
                print("ERROR: Map name cannot be empty")
                return False
            elif map not in cfgs.map_stores:
                print(f"ERROR: Map '{map}' not found")
                return False
            else:
                if map == "idmap" and not args.force:
                    print("ERROR: Cannot clear idmap without --force")
                    return False
                else:
                    cfgs.instantiate_map(map)["store"].clear()
                    print(f"Cleared map '{map}'")
                    return True

        if args.index:
            # Get the indexes from the source(s) from config
            indexes = []
            if args.index == "all":
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
            for idx in indexes:
                lmidx = idx["index"]
                # lmidx.open("w")
                # lmidx.index.drop()
                # lmidx.close()
                print(f"FIXME: Can't clear {idx['source']}/{idx['name']} directly? Just delete the files.")
            return True

        if not args.source:
            print("Which source(s) to clear must be given")
            return False
        elif args.source == "all":
            sources = [*cfgs.internal.keys(), *cfgs.external.keys(), *cfgs.results.keys()]
        elif args.source in ["internal", "external", "results"]:
            sources = list(getattr(cfgs, args.source).keys())
        else:
            sources = args.source.split(",")

        if not hasattr(args, "cache") or not args.cache:
            # default to non data caches
            caches = ["recordcache", "reconciledRecordcache", "recordcache2"]
        else:
            caches = args.cache.split(",")

        for s in sources:
            if s in cfgs.internal:
                cfg = cfgs.internal[s]
            elif s in cfgs.external:
                cfg = cfgs.external[s]
            elif s in cfgs.results:
                cfg = cfgs.results[s]
            elif s in ["all_refs", "done_refs", "networkmap"]:
                refs = cfgs.instantiate_map(s)["store"]
                print(f"Clearing map {s}")
                refs.clear()
                continue
            else:
                # uhh...
                print(f"Could not find cache to clear: {s}")
            for c in caches:
                cache = cfg.get(c, None)
                if cache is not None and hasattr(cache, "clear"):
                    print(f"Clearing {c} for {s} ")
                    cache.clear()
