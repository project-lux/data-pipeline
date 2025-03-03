

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
        else:
            # uhh...
            print(f"Could not find cache to clear: {s}")
        for c in caches:
            cache = cfg.get(c, None)
            if cache is not None and hasattr(cache, 'clear'):
                print(f"Clearing {c} for {s} ")
                cache.clear()
