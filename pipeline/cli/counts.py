def handle_command(cfgs, args):

    if not args.source:
        args.source = "all"
    if args.source == "all":
        sources = [*cfgs.internal.keys(), *cfgs.external.keys(), *cfgs.results.keys()]
    elif args.source in ['internal', 'external', 'results']:
        sources = list(getattr(cfgs, args.source).keys())
    else:
        sources = args.source.split(',')

    if not args.cache:
        # default to non data caches
        caches = ["datacache", "recordcache", "reconciledRecordcache", "recordcache2"]
    else:
        caches = args.cache.split(',')

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
    ttl = "Total Records Seen:".rjust(36)
    print(f"{ttl} ~{total}")
