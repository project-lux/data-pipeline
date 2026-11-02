"""Merge: the legacy entry point.

    python run-merge.py 0 24 --all              # slice 0 of 24, every internal source
    python run-merge.py --ycba --recid obj/1
    python run-merge.py 3 24 --all --norefs --resume
    python run-merge.py 0 24 --onlyrefs         # only the references pass

The work is lux_pipeline.process.phases.merge; this script parses sys.argv
as it always has and calls it. --profile is accepted and ignored
(refactor_docs/41).
"""
import os
import sys
from dotenv import load_dotenv
import logging
logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
from lux_pipeline.config import Config
from lux_pipeline.process.phases.merge import merge_slice, read_update_token, select_sources


def main():
    load_dotenv()
    basepath = os.getenv("LUX_BASEPATH", "")
    cfgs = Config(basepath=basepath)
    idmap = cfgs.get_idmap()
    cfgs.cache_globals()
    cfgs.instantiate_all()

    # --- process command line arguments ---

    if "--profile" in sys.argv:
        sys.argv.remove("--profile")
    if "--norefs" in sys.argv:
        sys.argv.remove("--norefs")
        do_references = False
    else:
        do_references = True
    resume = "--resume" in sys.argv

    max_slice = -1
    my_slice = -1
    recids = []
    while "--recid" in sys.argv:
        idx = sys.argv.index("--recid")
        recid = sys.argv[idx + 1]
        recids.append(recid)
        sys.argv.pop(idx)
        sys.argv.pop(idx)

    # --<src> names a configured source; any other --flag is ignored
    names = [a[2:] for a in sys.argv
             if a.startswith("--") and (a[2:] in cfgs.internal or a[2:] in cfgs.external)]
    try:
        read_update_token(cfgs)
        to_do = select_sources(cfgs, names=names, all_internal="--all" in sys.argv,
                               only_refs="--onlyrefs" in sys.argv, recids=recids)
    except ValueError as e:
        print(e)
        sys.exit(1)

    if len(sys.argv) > 2 and sys.argv[1].isnumeric() and sys.argv[2].isnumeric():
        my_slice = int(sys.argv[1])
        max_slice = int(sys.argv[2])

    merge_slice(cfgs, idmap, to_do, my_slice=my_slice, max_slice=max_slice, recids=recids,
                do_references=do_references, resume=resume)


if __name__ == "__main__":
    main()
