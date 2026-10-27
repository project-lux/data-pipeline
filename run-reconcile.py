"""Reconcile: the legacy entry point.

    python run-reconcile.py 0 24 --all              # slice 0 of 24, every internal source
    python run-reconcile.py --aat --recid 300010001
    python run-reconcile.py --baseline              # the global terms a new idmap needs
    python run-reconcile.py 3 24 --all --norefs

The work is lux_pipeline.process.phases.reconcile; this script parses
sys.argv as it always has and calls it. --profile is accepted and ignored
(refactor_docs/41).
"""
import os
import sys
from dotenv import load_dotenv
import logging
logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
from lux_pipeline.config import Config
from lux_pipeline.process.phases.reconcile import reconcile_slice, select_sources


def main():
    load_dotenv()
    basepath = os.getenv("LUX_BASEPATH", "")
    cfgs = Config(basepath=basepath)
    idmap = cfgs.get_idmap()
    networkmap = cfgs.instantiate_map("networkmap")["store"]
    cfgs.cache_globals()
    cfgs.instantiate_all()

    # --- process command line arguments ---

    my_slice = -1
    max_slice = -1

    if "--profile" in sys.argv:
        sys.argv.remove("--profile")
    if "--norefs" in sys.argv:
        sys.argv.remove("--norefs")
        DO_REFERENCES = False
    else:
        DO_REFERENCES = True

    if "--baseline" in sys.argv:
        to_do = select_sources(cfgs, baseline=True)
    else:
        # --<src> names a configured source; any other --flag is ignored
        names = [a[2:] for a in sys.argv
                 if a.startswith("--") and (a[2:] in cfgs.internal or a[2:] in cfgs.external)]

        recids = []
        while "--recid" in sys.argv:
            idx = sys.argv.index("--recid")
            recid = sys.argv[idx + 1]
            recids.append(recid)
            sys.argv.pop(idx)
            sys.argv.pop(idx)

        try:
            to_do = select_sources(cfgs, names=names, all_internal="--all" in sys.argv,
                                   recids=recids)
        except ValueError as e:
            print(e)
            sys.exit(1)

        if len(sys.argv) > 2 and sys.argv[1].isnumeric() and sys.argv[2].isnumeric():
            my_slice = int(sys.argv[1])
            max_slice = int(sys.argv[2])

    reconcile_slice(cfgs, idmap, networkmap, to_do, my_slice=my_slice, max_slice=max_slice,
                    do_references=DO_REFERENCES)


if __name__ == "__main__":
    main()
