# Roll up the per-slice timing JSON a build leaves behind.
#
#   python timing-report.py                       # every phase in the log dir
#   python timing-report.py --phase merge
#   python timing-report.py --dir /path/to/logs
#   python timing-report.py --compare old/ new/   # two runs side by side
#
# Works on a running build as well as a finished one: PhaseTimer rewrites its
# snapshot on every progress report, so a phase still in flight shows up with
# complete=false and partial-but-real numbers.

import argparse
import glob
import json
import os
import sys
from collections import defaultdict

from dotenv import load_dotenv


def load(directory, phase=None):
    out = defaultdict(list)
    for fn in sorted(glob.glob(os.path.join(directory, "timing-*.json"))):
        try:
            with open(fn) as fh:
                j = json.load(fh)
        except Exception as e:
            print(f"  (skipping {os.path.basename(fn)}: {e})")
            continue
        if phase and j.get("phase") != phase:
            continue
        out[j.get("phase", "?")].append(j)
    return out


def summarise(runs):
    """One phase, across its slices."""
    n = len(runs)
    records = sum(r["records"] for r in runs)
    skipped = sum(r.get("skipped", 0) for r in runs)
    # slices run concurrently, so the phase's wall clock is the slowest one,
    # while worker-seconds is what the stage percentages divide into
    wall = max(r["seconds"] for r in runs)
    worker_seconds = sum(r["seconds"] for r in runs)
    cpu = sum(r["cpu_seconds"] for r in runs)
    stages = defaultdict(lambda: [0, 0.0])
    for r in runs:
        for name, st in r.get("stages", {}).items():
            stages[name][0] += st["calls"]
            stages[name][1] += st["seconds"]
    accounted = sum(s[1] for s in stages.values())
    return {
        "slices": n,
        "complete": sum(1 for r in runs if r.get("complete")),
        "records": records,
        "skipped": skipped,
        "wall": wall,
        "worker_seconds": worker_seconds,
        "cpu_seconds": cpu,
        "cpu_percent": cpu / worker_seconds * 100 if worker_seconds else 0,
        "rate": records / wall if wall else 0,
        "stages": stages,
        "unattributed": worker_seconds - accounted,
        "runs": runs,
    }


def report(phase, s, stragglers=True):
    done = f"{s['complete']}/{s['slices']} finished"
    print(f"\n=== {phase}  ({done})")
    print(f"  {s['records']:,} records in {s['wall'] / 60:.1f} min wall "
          f"= {s['rate']:,.0f}/s aggregate"
          + (f", {s['skipped']:,} skipped" if s["skipped"] else ""))
    print(f"  {s['worker_seconds'] / 3600:.1f} worker-hours, "
          f"{s['cpu_seconds'] / 3600:.1f} cpu-hours = {s['cpu_percent']:.0f}% cpu")
    idle = 100 - s["cpu_percent"]
    if idle > 20:
        print(f"  workers are off-cpu {idle:.0f}% of the time -- the stages below "
              f"say what they are waiting in")

    if s["stages"]:
        print(f"\n  {'stage':<20} {'worker-hrs':>11} {'% worker':>9} "
              f"{'calls':>14} {'us/call':>10}")
        rows = sorted(s["stages"].items(), key=lambda kv: -kv[1][1])
        for name, (calls, secs) in rows:
            print(f"  {name:<20} {secs / 3600:>11.2f} "
                  f"{secs / s['worker_seconds'] * 100 if s['worker_seconds'] else 0:>8.1f}% "
                  f"{calls:>14,} {secs / calls * 1e6 if calls else 0:>10,.1f}")
        print(f"  {'(unattributed)':<20} {s['unattributed'] / 3600:>11.2f} "
              f"{s['unattributed'] / s['worker_seconds'] * 100 if s['worker_seconds'] else 0:>8.1f}%")

    if stragglers and s["slices"] > 2:
        rates = sorted((r["records_per_second"], r["slice"]) for r in s["runs"])
        median = rates[len(rates) // 2][0]
        slow = [(rt, sl) for rt, sl in rates if median and rt < median * 0.8]
        fast = [(rt, sl) for rt, sl in rates if median and rt > median * 1.2]
        spread = (rates[-1][0] - rates[0][0]) / median * 100 if median else 0
        print(f"\n  per-slice rate: {rates[0][0]:,.0f} to {rates[-1][0]:,.0f}/s "
              f"(median {median:,.0f}, spread {spread:.0f}%)")
        if slow:
            print(f"  slow slices (>20% under median): "
                  f"{', '.join(str(sl) for _, sl in slow)}")
        elif spread < 25:
            print(f"  evenly spread -- no straggler, so the work is partitioned well")
        if fast:
            print(f"  fast slices (>20% over median): "
                  f"{', '.join(str(sl) for _, sl in fast)}")

    marks = s["runs"][0].get("marks") or []
    if marks:
        print(f"  source boundaries (slice {s['runs'][0]['slice']}): "
              + ", ".join(f"{m[0]} at {m[1] / 60:.0f}m" for m in marks))


def compare(a_dir, b_dir, phase=None):
    a, b = load(a_dir, phase), load(b_dir, phase)
    for ph in sorted(set(a) | set(b)):
        if ph not in a or ph not in b:
            print(f"\n=== {ph}: only in {a_dir if ph in a else b_dir}")
            continue
        sa, sb = summarise(a[ph]), summarise(b[ph])
        print(f"\n=== {ph}")
        print(f"  {'':<20} {'before':>14} {'after':>14} {'change':>10}")
        for label, ka, kb, fmt in (
            ("wall minutes", sa["wall"] / 60, sb["wall"] / 60, "{:,.1f}"),
            ("records", sa["records"], sb["records"], "{:,.0f}"),
            ("records/s", sa["rate"], sb["rate"], "{:,.0f}"),
            ("cpu %", sa["cpu_percent"], sb["cpu_percent"], "{:,.0f}"),
        ):
            chg = (kb / ka - 1) * 100 if ka else 0
            print(f"  {label:<20} {fmt.format(ka):>14} {fmt.format(kb):>14} "
                  f"{chg:>+9.1f}%")
        names = set(sa["stages"]) | set(sb["stages"])
        print(f"\n  {'stage':<20} {'before hrs':>11} {'after hrs':>11} {'change':>10}")
        for name in sorted(names, key=lambda n: -sb["stages"].get(n, [0, 0])[1]):
            ha = sa["stages"].get(name, [0, 0.0])[1] / 3600
            hb = sb["stages"].get(name, [0, 0.0])[1] / 3600
            chg = (hb / ha - 1) * 100 if ha else float("inf")
            chgs = f"{chg:>+9.1f}%" if ha else "       new"
            print(f"  {name:<20} {ha:>11.2f} {hb:>11.2f} {chgs}")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dir", default=None, help="log dir (default: from config)")
    ap.add_argument("--phase", default=None, help="just this phase")
    ap.add_argument("--compare", nargs=2, metavar=("BEFORE", "AFTER"),
                    help="two directories of timing json, side by side")
    args = ap.parse_args()

    if args.compare:
        compare(args.compare[0], args.compare[1], args.phase)
        return

    directory = args.dir
    if directory is None:
        load_dotenv()
        from pipeline.config import Config
        cfgs = Config(basepath=os.getenv("LUX_BASEPATH", ""))
        directory = cfgs.log_dir
    print(f"# {directory}")
    phases = load(directory, args.phase)
    if not phases:
        print("  no timing-*.json found -- has a phase run since timing was added?")
        sys.exit(1)
    for ph in sorted(phases):
        report(ph, summarise(phases[ph]))


if __name__ == "__main__":
    main()
