# Report on a subsidiary identity map: what it took from its master, and what
# it has decided since.
#
#   python idmap-subsidiary.py                            # summary
#   python idmap-subsidiary.py --diff                     # every divergence
#   python idmap-subsidiary.py --diff --kind swapped
#   python idmap-subsidiary.py --diff -o diverged.tsv
#   python idmap-subsidiary.py --changes --since 2026-09-01
#   python idmap-subsidiary.py --map idmap_sub_places --vacuum
#
# Read-only unless --vacuum is given, and the master is read-only always --
# the store wraps it in a proxy that refuses writes. Nothing here can move an
# identity in production; --diff is the input to a decision to do that, taken
# elsewhere.
#
# --diff is derived from the origin snapshot outer-joined to the live rows, so
# it is the authoritative answer whatever happened to the audit trail, and a
# key that moved five times collapses to where it started and where it is now.
# --changes is the trail itself, in order, when the intermediate states matter.

import argparse
import os
import sys

from dotenv import load_dotenv

from pipeline.config import Config

DIFF_COLS = ("kind", "uri", "master_yuid", "local_yuid")
CHANGE_COLS = ("id", "ts", "op", "uri", "old_yuid", "new_yuid")


def rows_out(rows, cols, path):
    """TSV to a file or stdout. URIs, so nothing needs quoting -- but a stray
    tab would silently shift a column, so they are stripped rather than
    trusted."""
    fh = open(path, "w") if path else sys.stdout
    try:
        fh.write("\t".join(cols) + "\n")
        n = 0
        for row in rows:
            fh.write("\t".join(
                str(row.get(c, "") if row.get(c) is not None else "").replace("\t", " ")
                for c in cols) + "\n")
            n += 1
        return n
    finally:
        if path:
            fh.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--map", default="idmap_sub",
                    help="name of the subsidiary map store (default idmap_sub)")
    ap.add_argument("--diff", action="store_true",
                    help="list every URI whose identity differs from the master's")
    ap.add_argument("--kind", choices=["swapped", "added", "removed", "unseen"],
                    help="restrict --diff to one kind of divergence")
    ap.add_argument("--changes", action="store_true",
                    help="list the audit trail, oldest first")
    ap.add_argument("--since", help="with --changes: only entries at or after "
                                    "this timestamp")
    ap.add_argument("--op", action="append",
                    help="with --changes: restrict to this operation; repeatable")
    ap.add_argument("--vacuum", action="store_true",
                    help="VACUUM (ANALYZE) the map's tables. The only write here.")
    ap.add_argument("-o", "--out", help="write TSV here instead of stdout")
    args = ap.parse_args()

    load_dotenv()
    cfgs = Config(basepath=os.getenv("LUX_BASEPATH", ""))
    try:
        idmap = cfgs.instantiate_map(args.map)["store"]
    except ValueError as e:
        raise SystemExit(f"{e}: no map store called '{args.map}'")
    if not hasattr(idmap, "iter_divergence"):
        raise SystemExit(f"'{args.map}' is not a subsidiary idmap "
                         f"({type(idmap).__module__}.{type(idmap).__name__})")

    if args.diff:
        rows = idmap.iter_divergence()
        if args.kind:
            rows = (r for r in rows if r["kind"] == args.kind)
        n = rows_out(rows, DIFF_COLS, args.out)
        if args.out:
            print(f"{n:,} divergences -> {args.out}")
    elif args.changes:
        rows = idmap.iter_changes(since=args.since, ops=args.op)
        n = rows_out(rows, CHANGE_COLS, args.out)
        if args.out:
            print(f"{n:,} changes -> {args.out}")
    else:
        idmap.report()

    if args.vacuum:
        idmap.optimize()


if __name__ == "__main__":
    main()
