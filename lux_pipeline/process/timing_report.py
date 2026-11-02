"""Roll up the per-slice timing JSON (and cProfile output) a build leaves.

Every `PhaseTimer` writes ``timing-<phase>-<slice>.json`` to ``log_dir``,
and with profiling on ``profile-<phase>-<slice>.prof`` beside it. This adds
the slices of each phase up, stage by stage; `lux timing-report` (and the
root ``timing-report.py``) print what it returns.

Works on a running build as well as a finished one: PhaseTimer rewrites its
snapshot on every progress report, so a phase still in flight shows up with
complete=false and partial-but-real numbers.

The report functions return the report as lines rather than printing it;
files that cannot be read are logged and skipped.
"""

import glob
import json
import logging
import os
import pstats
from collections import defaultdict

# One definition of "a dotted name is nested inside its prefix", shared with
# the timer that writes the JSON -- see split_stages' docstring for what
# counting them twice did to the unattributed figure.
from lux_pipeline.process.timing import split_stages

logger = logging.getLogger("lux_pipeline")

DEFAULT_TOP = 20


def load(directory, phase=None):
    """``{phase: [timing json, ...]}`` for the ``timing-*.json`` files in
    `directory`, or only `phase`'s."""
    out = defaultdict(list)
    for fn in sorted(glob.glob(os.path.join(directory, "timing-*.json"))):
        try:
            with open(fn) as fh:
                j = json.load(fh)
        except Exception as e:
            logger.warning(f"skipping {os.path.basename(fn)}: {e}")
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
    # Only top-level stages are subtracted: a nested one ran inside its
    # parent, and counting both drove this negative (-53.1% on reconcile).
    top, kids = split_stages(stages)
    accounted = sum(s[1] for s in top.values())
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
        "top_stages": top,
        "nested_stages": kids,
        "unattributed": worker_seconds - accounted,
        "runs": runs,
    }


def slice_rates(s):
    """The spread of one phase's per-slice rates, or None for two slices or
    fewer: ``lowest``, ``highest``, ``median``, ``spread`` (% of the
    median), the ``slow`` (>20% under the median) and ``fast`` (>20% over)
    slice numbers, and ``even`` (no slow slice and under 25% spread)."""
    if s["slices"] <= 2:
        return None
    rates = sorted((r["records_per_second"], r["slice"]) for r in s["runs"])
    median = rates[len(rates) // 2][0]
    slow = [sl for rt, sl in rates if median and rt < median * 0.8]
    fast = [sl for rt, sl in rates if median and rt > median * 1.2]
    spread = (rates[-1][0] - rates[0][0]) / median * 100 if median else 0
    return {"lowest": rates[0][0], "highest": rates[-1][0], "median": median,
            "spread": spread, "slow": slow, "fast": fast,
            "even": not slow and spread < 25}


def nested_overrun(s, parent):
    """Worker-seconds by which `parent`'s nested stages add up to more than
    the parent itself (time attributed from inside other stages), or 0."""
    ptotal = s["stages"][parent][1]
    ctotal = sum(v[1] for v in s["nested_stages"][parent].values())
    return ctotal - ptotal if ctotal > ptotal * 1.001 else 0


def report_lines(phase, s, stragglers=True):
    """The report on one phase's summary `s` (from `summarise`)."""
    out = []
    done = f"{s['complete']}/{s['slices']} finished"
    out.append(f"\n=== {phase}  ({done})")
    out.append(f"  {s['records']:,} records in {s['wall'] / 60:.1f} min wall "
               f"= {s['rate']:,.0f}/s aggregate"
               + (f", {s['skipped']:,} skipped" if s["skipped"] else ""))
    out.append(f"  {s['worker_seconds'] / 3600:.1f} worker-hours, "
               f"{s['cpu_seconds'] / 3600:.1f} cpu-hours = {s['cpu_percent']:.0f}% cpu")
    idle = 100 - s["cpu_percent"]
    if idle > 20:
        out.append(f"  workers are off-cpu {idle:.0f}% of the time -- the stages below "
                   f"say what they are waiting in")

    if s["stages"]:
        out.append(f"\n  {'stage':<20} {'worker-hrs':>11} {'% worker':>9} "
                   f"{'calls':>14} {'us/call':>10}")
        ws = s["worker_seconds"]
        rows = sorted(s["top_stages"].items(), key=lambda kv: -kv[1][1])
        for name, (calls, secs) in rows:
            out.append(f"  {name:<20} {secs / 3600:>11.2f} "
                       f"{secs / ws * 100 if ws else 0:>8.1f}% "
                       f"{calls:>14,} {secs / calls * 1e6 if calls else 0:>10,.1f}")
        out.append(f"  {'(unattributed)':<20} {s['unattributed'] / 3600:>11.2f} "
                   f"{s['unattributed'] / ws * 100 if ws else 0:>8.1f}%")

        # Nested stages, reported but never subtracted. Deliberately not
        # indented under their parent in the same table: timing.stage()
        # attributes to whichever timer is active, so a helper called from
        # two stages puts time here that its nominal parent never contained.
        for parent, group in s["nested_stages"].items():
            ptotal = s["stages"][parent][1]
            over = nested_overrun(s, parent)
            out.append(f"\n  inside {parent} -- already counted above, not additional:")
            for name, (calls, secs) in sorted(group.items(), key=lambda kv: -kv[1][1]):
                out.append(f"    {name:<18} {secs / 3600:>11.2f} {'':>9} "
                           f"{calls:>14,} {secs / calls * 1e6 if calls else 0:>10,.1f}")
            if over:
                out.append(f"    ...totalling {(ptotal + over) / 3600:.2f} worker-hrs against "
                           f"{parent}'s {ptotal / 3600:.2f}, so {over / 3600:.2f} "
                           f"of it runs inside other stages -- not a breakdown of {parent}")

    sr = slice_rates(s) if stragglers else None
    if sr:
        out.append(f"\n  per-slice rate: {sr['lowest']:,.0f} to {sr['highest']:,.0f}/s "
                   f"(median {sr['median']:,.0f}, spread {sr['spread']:.0f}%)")
        if sr["slow"]:
            out.append(f"  slow slices (>20% under median): "
                       f"{', '.join(str(sl) for sl in sr['slow'])}")
        elif sr["even"]:
            out.append("  evenly spread -- no straggler, so the work is partitioned well")
        if sr["fast"]:
            out.append(f"  fast slices (>20% over median): "
                       f"{', '.join(str(sl) for sl in sr['fast'])}")

    marks = s["runs"][0].get("marks") or []
    if marks:
        out.append(f"  source boundaries (slice {s['runs'][0]['slice']}): "
                   + ", ".join(f"{m[0]} at {m[1] / 60:.0f}m" for m in marks))
    return out


# ------------------------------------------------------------- profiles


def load_profiles(directory, phase=None):
    """``{phase: [path, ...]}`` for the ``profile-<phase>-<slice>.prof``
    files PhaseTimer writes when profiling."""
    out = defaultdict(list)
    for fn in sorted(glob.glob(os.path.join(directory, "profile-*.prof"))):
        ph = os.path.basename(fn)[len("profile-"):-len(".prof")].rsplit("-", 1)[0]
        if phase and ph != phase:
            continue
        out[ph].append(fn)
    return out


def combine(paths):
    """One pstats.Stats for every slice's profile, or None if none load."""
    stats = None
    for fn in paths:
        try:
            if stats is None:
                stats = pstats.Stats(fn)
            else:
                stats.add(fn)
        except Exception as e:
            logger.warning(f"skipping {os.path.basename(fn)}: {e}")
    return stats


def func_label(func):
    """``name (dir/file.py:line)`` from a pstats key."""
    filename, line, name = func
    if filename == "~":
        return name     # a builtin: "<method 'execute' ...>"
    parts = filename.split(os.sep)
    short = os.sep.join(parts[-2:]) if len(parts) > 1 else filename
    return f"{name} ({short}:{line})"


def top_functions(stats, key, n):
    """``[(func, calls, tottime, cumtime), ...]``, the `n` largest by `key`
    (``"cumtime"`` or ``"tottime"``)."""
    rows = [(f, nc, tt, ct) for f, (cc, nc, tt, ct, _callers) in stats.stats.items()]
    idx = 2 if key == "tottime" else 3
    return sorted(rows, key=lambda r: -r[idx])[:n]


def stage_functions(stats, stage):
    """Profiled functions whose name is the stage's (its last dotted part:
    ``acquire.map`` is ``map``), largest cumulative time first."""
    name = stage.rsplit(".", 1)[-1]
    found = [(f, st[3]) for f, st in stats.stats.items() if f[2] == name]
    return [f for f, _ in sorted(found, key=lambda x: -x[1])]


def profile_data(paths, stages=(), n=DEFAULT_TOP):
    """One phase's profiles added up across its slices, or None if none
    load: ``slices``, ``calls``, ``cpu_seconds``, ``top`` (``{"cumtime":
    rows, "tottime": rows}``, rows from `top_functions`) and ``matched``:
    ``[(stage, func, cumtime, [(callee, share %, cumtime), ...]), ...]``
    for each stage with a function of the same name (the largest), and
    the five callees it spends most in."""
    stats = combine(paths)
    if stats is None:
        return None
    out = {"slices": len(paths), "calls": stats.total_calls,
           "cpu_seconds": stats.total_tt, "n": n,
           "top": {key: top_functions(stats, key, n) for key in ("cumtime", "tottime")},
           "matched": []}
    # a stage is a `with timer.stage(name)` block, which the profiler can't
    # see; where a function has the stage's name, show what it spends on
    matched = [(st, fs[0]) for st in stages if (fs := stage_functions(stats, st))]
    if matched:
        stats.calc_callees()
        for st, f in matched:
            ct = stats.stats[f][3]
            callees = stats.all_callees.get(f, {})
            rows = sorted(callees.items(), key=lambda kv: -kv[1][3])[:5]
            out["matched"].append((st, f, ct, [(g, cct / ct * 100 if ct else 0, cct)
                                               for g, (cc, nc, tt, cct) in rows]))
    return out


PROFILE_TITLES = {"cumtime": "cumulative (the function and all it calls)",
                  "tottime": "internal (the function's own code)"}


def profile_lines(phase, paths, stages=(), n=DEFAULT_TOP):
    """The top `n` functions of one phase's profiles, added up across its
    slices, and what each stage's same-named function spends its time on."""
    return profile_data_lines(phase, profile_data(paths, stages, n))


def profile_data_lines(phase, p):
    """`profile_data`'s result `p` for one phase, as lines."""
    if p is None:
        return []
    n = p["n"]
    out = [f"\n=== {phase} profile  ({p['slices']} slice{'s' if p['slices'] != 1 else ''}, "
           f"{p['calls']:,} calls, {p['cpu_seconds'] / 3600:.2f} profiled cpu-hours)"]
    for key, title in PROFILE_TITLES.items():
        out.append(f"\n  top {n} by {title}")
        out.append(f"  {'calls':>12} {'own s':>10} {'cum s':>10} {'us/call':>9}  function")
        for f, nc, tt, ct in p["top"][key]:
            per = (ct if key == "cumtime" else tt) / nc * 1e6 if nc else 0
            out.append(f"  {nc:>12,} {tt:>10,.1f} {ct:>10,.1f} {per:>9,.1f}  {func_label(f)}")
    if p["matched"]:
        out.append("\n  stages matched to a function of the same name (the largest one):")
        for st, f, ct, callees in p["matched"]:
            out.append(f"\n  {st}: {func_label(f)}  {ct:,.1f}s cumulative")
            for g, share, cct in callees:
                out.append(f"      {share:>5.1f}%  {cct:>10,.1f}s  {func_label(g)}")
    return out


# ------------------------------------------------------------- reports


def _check_dir(directory):
    if not os.path.isdir(directory):
        raise FileNotFoundError(f"no such directory: {directory}")


def directory_data(directory, phase=None, profile=None):
    """``[(phase, summary, profile data or None), ...]`` for every phase (or
    `phase`) in `directory`, in name order, and whether profiles were asked
    for but none found. `profile` is the number of top functions to take
    from the ``profile-*.prof`` files, or None for none. Raises when there
    is no timing JSON to report on."""
    _check_dir(directory)
    phases = load(directory, phase)
    if not phases:
        which = f"timing-{phase}-*.json" if phase else "timing-*.json"
        raise RuntimeError(f"no {which} in {directory} -- has a phase run since "
                           f"timing was added?")
    profiles = load_profiles(directory, phase) if profile else {}
    out = []
    for ph in sorted(phases):
        s = summarise(phases[ph])
        prof = (profile_data(profiles[ph], stages=list(s["stages"]), n=profile)
                if ph in profiles else None)
        out.append((ph, s, prof))
    return out, bool(profile and not profiles)


NO_PROFILES = "no profile-*.prof found -- run the phase with --profile or LUX_PROFILE=1"


def directory_report(directory, phase=None, profile=None):
    """The report on every phase (or `phase`) in `directory`, as lines;
    see `directory_data`."""
    phases, no_profiles = directory_data(directory, phase, profile)
    out = [f"# {directory}"]
    if no_profiles:
        out.append(f"  ({NO_PROFILES})")
    for ph, s, prof in phases:
        out.extend(report_lines(ph, s))
        out.extend(profile_data_lines(ph, prof))
    return out


def compare_data(a_dir, b_dir, phase=None):
    """Two runs' timing JSON side by side: one dict per phase in either,
    in name order. A phase in only one has ``only_in`` (that directory);
    otherwise ``metrics``, ``[(label, before, after, change %), ...]``, and
    ``stages``, ``[(name, before hrs, after hrs, change % or None for a new
    stage), ...]`` largest-after first."""
    _check_dir(a_dir)
    _check_dir(b_dir)
    a, b = load(a_dir, phase), load(b_dir, phase)
    out = []
    for ph in sorted(set(a) | set(b)):
        if ph not in a or ph not in b:
            out.append({"phase": ph, "only_in": a_dir if ph in a else b_dir})
            continue
        sa, sb = summarise(a[ph]), summarise(b[ph])
        metrics = []
        for label, ka, kb in (
            ("wall minutes", sa["wall"] / 60, sb["wall"] / 60),
            ("records", sa["records"], sb["records"]),
            ("records/s", sa["rate"], sb["rate"]),
            ("cpu %", sa["cpu_percent"], sb["cpu_percent"]),
        ):
            metrics.append((label, ka, kb, (kb / ka - 1) * 100 if ka else 0))
        stages = []
        names = set(sa["stages"]) | set(sb["stages"])
        # largest after first; ties by name, not by the set's hash order
        for name in sorted(names, key=lambda n: (-sb["stages"].get(n, [0, 0])[1], n)):
            ha = sa["stages"].get(name, [0, 0.0])[1] / 3600
            hb = sb["stages"].get(name, [0, 0.0])[1] / 3600
            stages.append((name, ha, hb, (hb / ha - 1) * 100 if ha else None))
        out.append({"phase": ph, "only_in": None, "metrics": metrics, "stages": stages})
    return out


METRIC_FORMATS = {"wall minutes": "{:,.1f}", "records": "{:,.0f}",
                  "records/s": "{:,.0f}", "cpu %": "{:,.0f}"}


def compare_lines(a_dir, b_dir, phase=None):
    """Two runs' timing JSON side by side, phase by phase, as lines."""
    out = []
    for c in compare_data(a_dir, b_dir, phase):
        if c["only_in"]:
            out.append(f"\n=== {c['phase']}: only in {c['only_in']}")
            continue
        out.append(f"\n=== {c['phase']}")
        out.append(f"  {'':<20} {'before':>14} {'after':>14} {'change':>10}")
        for label, ka, kb, chg in c["metrics"]:
            fmt = METRIC_FORMATS[label]
            out.append(f"  {label:<20} {fmt.format(ka):>14} {fmt.format(kb):>14} "
                       f"{chg:>+9.1f}%")
        out.append(f"\n  {'stage':<20} {'before hrs':>11} {'after hrs':>11} {'change':>10}")
        for name, ha, hb, chg in c["stages"]:
            chgs = f"{chg:>+9.1f}%" if chg is not None else "       new"
            out.append(f"  {name:<20} {ha:>11.2f} {hb:>11.2f} {chgs}")
    if not out:
        out.append(f"  no timing json in either {a_dir} or {b_dir}")
    return out
