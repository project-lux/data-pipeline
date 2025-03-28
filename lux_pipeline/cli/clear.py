from argparse import ArgumentParser

def handle_command(cfgs, args, rest):

    if not args.source:
        print("Which source(s) to clear must be given")
        return False
    elif args.source == "all":
        sources = [*cfgs.internal.keys(), *cfgs.external.keys(), *cfgs.results.keys()]
    elif args.source in ['internal', 'external', 'results']:
        sources = list(getattr(cfgs, args.source).keys())
    else:
        sources = args.source.split(',')

    ap = ArgumentParser()
    ap.add_argument("--cache", type=str, help="Types of cache separated by commas, or 'all'")
    ap.parse_args(rest, namespace=args)

    if not args.cache:
        # default to non data caches
        caches = ["recordcache", "reconciledRecordcache", "recordcache2"]
    else:
        caches = args.cache.split(',')

    for s in sources:
        if s in cfgs.internal:
            cfg = cfgs.internal[s]
        elif s in cfgs.external:
            cfg = cfgs.external[s]
        elif s in cfgs.results:
            cfg = cfgs.results[s]
        elif s in ['all_refs', 'done_refs', 'networkmap']:
            refs = cfgs.instantiate_map(s)["store"]
            print(f"Clearing map {s}")
            refs.clear()
            continue
        else:
            # uhh...
            print(f"Could not find cache to clear: {s}")
        for c in caches:
            cache = cfg.get(c, None)
            if cache is not None and hasattr(cache, 'clear'):
                print(f"Clearing {c} for {s} ")
                cache.clear()
