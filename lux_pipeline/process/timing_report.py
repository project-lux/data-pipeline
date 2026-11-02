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
import time
from collections import defaultdict

# One definition of "a dotted name is nested inside its prefix", shared with
# the timer that writes the JSON -- see split_stages' docstring for what
# counting them twice did to the unattributed figure.
from lux_pipeline.process import host_cpu
from lux_pipeline.process.timing import split_stages

logger = logging.getLogger("lux_pipeline")

DEFAULT_TOP = 20

# The order a build runs its phases in (lux build, run-all.sh): the start-up
# steps, reconcile and its references, identify, merge and its references,
# the portal pass, then the exports. Reports list phases in this order; a
# phase not named here comes after them, alphabetically.
PHASE_ORDER = (
    "download", "harvest", "load", "load-index", "clear", "sheets",
    "reconcile", "reconcile-refs", "identify", "metatypes", "write-refs",
    "merge", "merge-refs", "portal",
    "export", "export-merged", "export-nt", "export-sources", "export-external",
)


def phase_key(phase):
    """Sort key putting phases in build order (`PHASE_ORDER`), unknown ones
    last by name."""
    try:
        return (PHASE_ORDER.index(phase), "")
    except ValueError:
        return (len(PHASE_ORDER), phase)


def load(directory, phase=None, stale=None):
    """``{phase: [timing json, ...]}`` for the ``timing-*.json`` files in
    `directory`, or only `phase`'s: the phase's latest run only.

    A run rewrites ``timing-<phase>-<slice>.json`` for the slices it has, so
    files of an earlier run survive beside it when that run had more slices
    (``-24`` .. ``-31`` after a 32-way run, beside a 24-way one) or none
    (``-all``, a ``--recid`` or single-process run). They differ from the
    latest run in ``max_slice``, so a phase keeps the files whose
    ``max_slice`` is that of its most recently updated file; the names of
    the others go into `stale` (``{phase: [file name, ...]}``) if given."""
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
        if "updated_at" not in j:
            # written before PhaseTimer recorded clock times: the file is
            # rewritten on every progress report and at the end, so its
            # mtime is (about) when the snapshot was taken
            try:
                j["updated_at"] = os.path.getmtime(fn)
            except OSError:
                j["updated_at"] = None
            j["times_estimated"] = True
        if "started_at" not in j and j.get("updated_at") is not None:
            j["started_at"] = j["updated_at"] - j.get("seconds", 0)
        j["file"] = os.path.basename(fn)
        out[j.get("phase", "?")].append(j)
    for ph, runs in out.items():
        newest = max(runs, key=lambda r: r.get("updated_at") or 0)
        keep = [r for r in runs if r.get("max_slice") == newest.get("max_slice")]
        if len(keep) < len(runs):
            dropped = sorted(r["file"] for r in runs if r not in keep)
            logger.debug(f"{ph}: ignoring {len(dropped)} file(s) of an earlier run: "
                         f"{', '.join(dropped)}")
            if stale is not None:
                stale[ph] = dropped
            out[ph] = keep
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
    starts = [r["started_at"] for r in runs if r.get("started_at") is not None]
    ends = [r["updated_at"] for r in runs if r.get("updated_at") is not None]
    first, last = host_window(runs)
    return {
        "host": host_cpu.delta(first, last),
        "started_at": min(starts) if starts else None,
        "ended_at": max(ends) if ends else None,
        "times_estimated": any(r.get("times_estimated") for r in runs),
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


def stale_note(files):
    """What the report says about the files `load` set aside."""
    shown = ", ".join(files[:4]) + (f", ... ({len(files) - 4} more)" if len(files) > 4 else "")
    return (f"{len(files)} file{'s' if len(files) != 1 else ''} of an earlier run with a "
            f"different number of slices: {shown}")


def host_window(runs):
    """The earliest start and latest snapshot `host_cpu` readings among
    `runs` (timing JSON), or ``(None, None)`` for files without them."""
    readings = [r["host_cpu"] for r in runs if r.get("host_cpu")]
    starts = [h["start"] for h in readings if h.get("start")]
    nows = [h["now"] for h in readings if h.get("now")]
    if not starts or not nows:
        return None, None
    return min(starts, key=lambda h: h["at"]), max(nows, key=lambda h: h["at"])


def postgres_meanwhile(h):
    """`` (postgres kept N cores busy meanwhile)`` for the off-cpu hint, or
    nothing without a postgres reading."""
    if not h or h.get("postgres_cores") is None:
        return ""
    return f" (postgres kept {h['postgres_cores']:.1f} cores busy meanwhile)"


def host_line(h, python_cpu):
    """Postgres's and the machine's cpu over a window, beside the workers'
    `python_cpu` seconds, as one line; None without readings."""
    if not h:
        return None
    parts = []
    if h["postgres"] is not None:
        parts.append(f"postgres {h['postgres'] / 3600:.1f} cpu-hours "
                     f"({h['postgres_cores']:.1f} cores) beside the workers' "
                     f"{python_cpu / 3600:.1f}")
    if h["busy_percent"] is not None:
        parts.append(f"machine {h['busy_percent']:.0f}% busy"
                     + (f", {h['iowait_percent']:.0f}% iowait"
                        if h["iowait_percent"] is not None else "")
                     + (f" over {h['cpus']} cpus" if h["cpus"] else ""))
    return "; ".join(parts)


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
    hl = host_line(s.get("host"), s["cpu_seconds"])
    if hl:
        out.append(f"  {hl}")
    idle = 100 - s["cpu_percent"]
    if s.get("stale_files"):
        out.append(f"  ignored {stale_note(s['stale_files'])}")
    if idle > 20:
        out.append(f"  workers are off-cpu {idle:.0f}% of the time -- the stages below "
                   f"say what they are waiting in" + postgres_meanwhile(s.get("host")))

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


# ------------------------------------------------------------- all phases

TOP_BOTTLENECKS = 5
# consecutive phases further apart than this are probably not one build
GAP_WARNING = 6 * 3600
# a phase's wait on its slowest slices worth reporting, as a share of its wall
STRAGGLER_SHARE = 0.10


def fmt_duration(secs):
    """``4d 02h``, ``2h 05m``, ``14m 30s`` or ``45s``."""
    secs = int(round(max(secs, 0)))
    d, rem = divmod(secs, 86400)
    if d:
        return f"{d}d {rem // 3600:02d}h"
    h, rem = divmod(secs, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m:02d}m"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def fmt_time(epoch):
    """Local time, to the minute."""
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(epoch))


def _union(intervals):
    """Total length covered by `intervals` ((start, end) pairs)."""
    total = 0.0
    cur_s = cur_e = None
    for s, e in sorted(intervals):
        if cur_e is None or s > cur_e:
            if cur_e is not None:
                total += cur_e - cur_s
            cur_s, cur_e = s, e
        else:
            cur_e = max(cur_e, e)
    if cur_e is not None:
        total += cur_e - cur_s
    return total


def overall_summary(phases, top=TOP_BOTTLENECKS):
    """All the phases together, or None for fewer than two. `phases` is
    `directory_data`'s ``[(phase, summary, profile), ...]`` (or
    ``(phase, summary)`` pairs).

    * ``started_at`` / ``ended_at`` / ``elapsed``: first start to last end
      (None without clock times); ``in_phases``, the part of it some phase
      was running; ``between``, the rest -- untimed steps and waiting
    * ``timeline``: ``(phase, start, end, wall, complete)`` in build order
    * ``gaps``: ``(after, before, seconds)`` for consecutive phases more
      than `GAP_WARNING` apart, which are likely from different builds
    * ``wall``: the phases' wall clocks added up; ``worker_seconds``,
      ``cpu_seconds``, ``cpu_percent`` across them all (the workers' own)
    * ``host``: `host_cpu.delta` from the first phase's start to the last
      phase's end -- postgres's cpu and the machine's -- or None
    * ``bottlenecks``: the `top` largest ``(phase, stage, seconds, share)``,
      where seconds is the phase's wall clock apportioned by the stage's
      share of its worker time -- the elapsed time it accounts for -- and
      share is that of the summed wall; ``(unattributed)`` competes too
    * ``stragglers``: ``(phase, seconds, slow slices)`` where the phase
      waited on its slowest slice for more than `STRAGGLER_SHARE` of its
      wall clock (slowest slice's time less the median slice's)
    * ``spread_out``: ``(phase, span, wall)`` for phases whose slices ran
      across far more time than the phase's wall clock (over three times,
      and more than `GAP_WARNING` longer): slices of different runs, or a
      run restarted slice by slice
    * ``incomplete``: phases not every slice of which finished
    * ``times_estimated``: some clock times came from file mtimes
    """
    rows = [(p[0], p[1]) for p in phases]
    if len(rows) < 2:
        return None
    timed = [(ph, s) for ph, s in rows if s["started_at"] is not None and s["ended_at"] is not None]
    timeline = sorted(((ph, s["started_at"], s["ended_at"], s["wall"],
                        s["complete"] == s["slices"]) for ph, s in timed),
                      key=lambda r: phase_key(r[0]))
    started = min((r[1] for r in timeline), default=None)
    ended = max((r[2] for r in timeline), default=None)
    elapsed = ended - started if timeline else None
    in_phases = _union([(r[1], r[2]) for r in timeline]) if timeline else None

    gaps = []
    reach = None
    prev = None
    for r in sorted(timeline, key=lambda r: r[1]):
        if reach is not None and r[1] - reach > GAP_WARNING:
            gaps.append((prev, r[0], r[1] - reach))
        if reach is None or r[2] > reach:
            reach, prev = r[2], r[0]

    # the whole span at once: phases overlap (merge-refs runs while the
    # last merge slices finish), so their own windows would count it twice
    windows = [host_window(s["runs"]) for _, s in rows]
    firsts = [w[0] for w in windows if w[0]]
    lasts = [w[1] for w in windows if w[1]]
    host = (host_cpu.delta(min(firsts, key=lambda h: h["at"]), max(lasts, key=lambda h: h["at"]))
            if firsts and lasts else None)
    wall = sum(s["wall"] for _, s in rows)
    worker = sum(s["worker_seconds"] for _, s in rows)
    cpu = sum(s["cpu_seconds"] for _, s in rows)

    cands = []
    for ph, s in rows:
        ws = s["worker_seconds"]
        if not ws:
            continue
        for name, (_calls, secs) in s["top_stages"].items():
            cands.append((ph, name, secs / ws * s["wall"]))
        if s["unattributed"] > 0:
            cands.append((ph, "(unattributed)", s["unattributed"] / ws * s["wall"]))
    cands.sort(key=lambda c: -c[2])
    bottlenecks = [(ph, name, secs, secs / wall if wall else 0)
                   for ph, name, secs in cands[:top]]

    stragglers = []
    for ph, s in rows:
        if s["slices"] < 3:
            continue
        secs = sorted(r["seconds"] for r in s["runs"])
        wait = secs[-1] - secs[len(secs) // 2]
        if s["wall"] and wait > s["wall"] * STRAGGLER_SHARE:
            sr = slice_rates(s)
            stragglers.append((ph, wait, sr["slow"] if sr else []))
    stragglers.sort(key=lambda x: -x[1])

    spread_out = [(ph, e - s, w) for ph, s, e, w, _done in timeline
                  if e - s > max(3 * w, w + GAP_WARNING)]

    return {
        "phases": len(rows),
        "started_at": started, "ended_at": ended, "elapsed": elapsed,
        "in_phases": in_phases,
        "between": elapsed - in_phases if timeline else None,
        "timeline": timeline, "gaps": gaps,
        "wall": wall, "worker_seconds": worker, "cpu_seconds": cpu, "host": host,
        "cpu_percent": cpu / worker * 100 if worker else 0,
        "bottlenecks": bottlenecks, "stragglers": stragglers, "spread_out": spread_out,
        "incomplete": [ph for ph, s in rows if s["complete"] < s["slices"]],
        "times_estimated": any(s.get("times_estimated") for _, s in rows),
    }


def spread_note(phase, span, wall):
    return (f"{phase}'s slices span {fmt_duration(span)} but its wall clock is "
            f"{fmt_duration(wall)} -- slices from more than one run?")


ESTIMATED_TIMES = ("some start and end times are estimated from the files' modification "
                   "times (written before PhaseTimer recorded them)")


def summary_lines(t):
    """`overall_summary`'s result `t` as lines; none for None."""
    if t is None:
        return []
    out = [f"\n=== all {t['phases']} phases"]
    if t["elapsed"] is not None:
        out.append(f"  {fmt_time(t['started_at'])} to {fmt_time(t['ended_at'])}: "
                   f"{fmt_duration(t['elapsed'])} start to end")
        out.append(f"  {fmt_duration(t['in_phases'])} inside a timed phase, "
                   f"{fmt_duration(t['between'])} between phases")
    out.append(f"  {fmt_duration(t['wall'])} of phase wall clock added up, "
               f"{t['worker_seconds'] / 3600:.1f} worker-hours, "
               f"{t['cpu_percent']:.0f}% cpu")
    hl = host_line(t["host"], t["cpu_seconds"])
    if hl:
        out.append(f"  start to end: {hl}")
    if t["timeline"]:
        out.append(f"\n  {'phase':<20} {'started':>16} {'finished':>16} {'wall':>9}")
        for ph, s, e, w, done in t["timeline"]:
            out.append(f"  {ph:<20} {fmt_time(s):>16} {fmt_time(e):>16} "
                       f"{fmt_duration(w):>9}" + ("" if done else "  (not finished)"))
    for a, b, secs in t["gaps"]:
        out.append(f"  {fmt_duration(secs)} between {a} and {b} -- probably not the same build")
    for ph, span, w in t["spread_out"]:
        out.append(f"  {spread_note(ph, span, w)}")
    if t["bottlenecks"]:
        out.append("\n  biggest bottlenecks (elapsed time the stage accounts for):")
        for ph, name, secs, share in t["bottlenecks"]:
            out.append(f"    {ph + ' / ' + name:<36} {fmt_duration(secs):>9} "
                       f"{share * 100:>5.1f}% of all phases")
    for ph, wait, slow in t["stragglers"]:
        out.append(f"  {ph} waited {fmt_duration(wait)} on its slowest slice"
                   + (f" (slow: {', '.join(map(str, slow))})" if slow else ""))
    if t["incomplete"]:
        out.append(f"  not finished: {', '.join(t['incomplete'])}")
    if t["times_estimated"]:
        out.append(f"  ({ESTIMATED_TIMES})")
    return out


# ------------------------------------------------------------- reports


def _check_dir(directory):
    if not os.path.isdir(directory):
        raise FileNotFoundError(f"no such directory: {directory}")


def directory_data(directory, phase=None, profile=None):
    """``[(phase, summary, profile data or None), ...]`` for every phase (or
    `phase`) in `directory`, in build order (`phase_key`), and whether profiles were asked
    for but none found. `profile` is the number of top functions to take
    from the ``profile-*.prof`` files, or None for none. Raises when there
    is no timing JSON to report on."""
    _check_dir(directory)
    stale = {}
    phases = load(directory, phase, stale)
    if not phases:
        which = f"timing-{phase}-*.json" if phase else "timing-*.json"
        raise RuntimeError(f"no {which} in {directory} -- has a phase run since "
                           f"timing was added?")
    profiles = load_profiles(directory, phase) if profile else {}
    out = []
    for ph in sorted(phases, key=phase_key):
        s = summarise(phases[ph])
        s["stale_files"] = stale.get(ph, [])
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
    out.extend(summary_lines(overall_summary(phases)))
    return out


def compare_data(a_dir, b_dir, phase=None):
    """Two runs' timing JSON side by side: one dict per phase in either,
    in build order. A phase in only one has ``only_in`` (that directory);
    otherwise ``metrics``, ``[(label, before, after, change %), ...]``, and
    ``stages``, ``[(name, before hrs, after hrs, change % or None for a new
    stage), ...]`` largest-after first."""
    _check_dir(a_dir)
    _check_dir(b_dir)
    a, b = load(a_dir, phase), load(b_dir, phase)
    out = []
    for ph in sorted(set(a) | set(b), key=phase_key):
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
