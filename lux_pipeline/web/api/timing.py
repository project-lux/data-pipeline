"""The timing report as the page shows it: `lux timing-report`'s numbers
(`process/utils/timing_report.directory_data`, `overall_summary`,
`compare_data`), shaped so the template does no arithmetic: a card per
phase in build order, its stages with their share of worker time, the
nested stages, the slices, the profiles; the all-phases summary with a
timeline; two runs side by side.

The directories read are ``log_dir`` by default; another one, for an
earlier build's copy, must be inside the installation (base, data, log,
exports or backup directory).
"""

import os

from fastapi import APIRouter, HTTPException, Request

from lux_pipeline.process.utils import timing_report as tr
from lux_pipeline.web.security import under

router = APIRouter(prefix="/api/timing", tags=["timing"])
ROOTS = ("base_dir", "data_dir", "log_dir", "exports_dir", "backup_dir")
# compare: whether a fall is an improvement
LOWER_IS_BETTER = {"wall minutes": True, "records/s": False, "cpu %": False, "records": None}


def timing_dir(state, path=None):
    """`path` (default ``log_dir``) when it is a directory inside the
    installation; 404 otherwise."""
    cfgs = state.configs()
    path = path or getattr(cfgs, "log_dir", None)
    if not path:
        raise HTTPException(status_code=404, detail="no log_dir in the configuration")
    roots = [getattr(cfgs, r, None) for r in ROOTS]
    if not any(under(r, path) for r in roots if r) or not os.path.isdir(path):
        raise HTTPException(status_code=404,
                            detail=f"{path} is not a directory inside the installation")
    return os.path.realpath(path)


def cpu_class(pct):
    return "ok" if pct >= 80 else "warn" if pct >= 50 else "fail"


def profile_view(p):
    if not p:
        return None
    rows = {key: [{"label": tr.func_label(f), "calls": nc, "tottime": tt, "cumtime": ct}
                  for f, nc, tt, ct in p["top"][key]] for key in ("cumtime", "tottime")}
    matched = [{"stage": st, "label": tr.func_label(f), "cumtime": ct,
                "callees": [{"label": tr.func_label(g), "share": share, "cumtime": cct}
                            for g, share, cct in callees]}
               for st, f, ct, callees in p["matched"]]
    return {"slices": p["slices"], "calls": p["calls"], "cpu_seconds": p["cpu_seconds"],
            "top": rows, "titles": tr.PROFILE_TITLES, "matched": matched}


def phase_view(phase, s, prof=None):
    """One phase as its card shows it."""
    ws = s["worker_seconds"]
    stages = [{"name": name, "hours": secs / 3600, "share": secs / ws * 100 if ws else 0,
               "calls": calls, "us_per_call": secs / calls * 1e6 if calls else None}
              for name, (calls, secs) in sorted(s["top_stages"].items(), key=lambda kv: -kv[1][1])]
    if s["stages"]:
        stages.append({"name": "(unattributed)", "hours": s["unattributed"] / 3600,
                       "share": s["unattributed"] / ws * 100 if ws else 0, "calls": None,
                       "us_per_call": None})
    nested = []
    for parent, group in s["nested_stages"].items():
        over = tr.nested_overrun(s, parent)
        ptotal = s["stages"][parent][1]
        nested.append({"parent": parent,
                       "rows": [{"name": n, "hours": secs / 3600, "calls": calls,
                                 "us_per_call": secs / calls * 1e6 if calls else None}
                                for n, (calls, secs) in sorted(group.items(), key=lambda kv: -kv[1][1])],
                       "overrun": (f"{(ptotal + over) / 3600:.2f} worker-hours against {parent}'s "
                                   f"{ptotal / 3600:.2f}: {over / 3600:.2f} of it runs inside other "
                                   f"stages, so this is not a breakdown of {parent}") if over else None})
    sr = tr.slice_rates(s)
    slow, fast = set((sr or {}).get("slow", [])), set((sr or {}).get("fast", []))
    slices = [{"slice": r.get("slice"), "records": r.get("records"),
               "seconds": r.get("seconds"), "rate": r.get("records_per_second"),
               "complete": bool(r.get("complete")),
               "class": "slow" if r.get("slice") in slow else "fast" if r.get("slice") in fast else ""}
              for r in sorted(s["runs"], key=lambda r: (r.get("slice") is None, r.get("slice") or 0))]
    idle = 100 - s["cpu_percent"]
    marks = s["runs"][0].get("marks") or []
    return {
        "phase": phase, "complete": s["complete"], "slices": s["slices"],
        "finished": s["complete"] == s["slices"], "records": s["records"], "skipped": s["skipped"],
        "wall": s["wall"], "rate": s["rate"], "worker_hours": ws / 3600,
        "cpu_hours": s["cpu_seconds"] / 3600, "cpu_percent": s["cpu_percent"],
        "cpu_class": cpu_class(s["cpu_percent"]), "started_at": s["started_at"],
        "ended_at": s["ended_at"], "host": tr.host_line(s.get("host"), s["cpu_seconds"]),
        "idle": (f"workers are off-cpu {idle:.0f}% of the time: the stages say what they are "
                 f"waiting in" + tr.postgres_meanwhile(s.get("host"))) if idle > 20 else None,
        "stale": tr.stale_note(s["stale_files"]) if s.get("stale_files") else None,
        "stages": stages, "nested": nested, "rates": sr, "slice_rows": slices,
        "marks": [{"source": m[0], "minutes": m[1] / 60} for m in marks],
        "marks_slice": s["runs"][0].get("slice"), "profile": profile_view(prof),
    }


def summary_view(t):
    """The all-phases summary, with the timeline as positions on a 0-1000
    axis for an SVG."""
    if not t:
        return None
    rows = []
    start, elapsed = t["started_at"], t["elapsed"]
    for i, (ph, s, e, wall, done) in enumerate(t["timeline"]):
        x = (s - start) / elapsed * 1000 if elapsed else 0
        w = max(2.0, (e - s) / elapsed * 1000) if elapsed else 1000
        rows.append({"phase": ph, "start": s, "end": e, "wall": wall, "done": done,
                     "x": round(x, 1), "w": round(min(w, 1000 - x), 1), "y": i * 22})
    return {
        "phases": t["phases"], "started_at": start, "ended_at": t["ended_at"], "elapsed": elapsed,
        "in_phases": t["in_phases"], "between": t["between"], "timeline": rows,
        "height": max(22, len(rows) * 22),
        "gaps": [{"after": a, "before": b, "seconds": secs} for a, b, secs in t["gaps"]],
        "wall": t["wall"], "worker_hours": t["worker_seconds"] / 3600,
        "cpu_percent": t["cpu_percent"], "cpu_class": cpu_class(t["cpu_percent"]),
        "host": tr.host_line(t.get("host"), t["cpu_seconds"]),
        "bottlenecks": [{"phase": ph, "stage": st, "seconds": secs, "share": share * 100}
                        for ph, st, secs, share in t["bottlenecks"]],
        "stragglers": [{"phase": ph, "seconds": secs, "slow": slow} for ph, secs, slow in t["stragglers"]],
        "spread_out": [{"phase": ph, "span": span, "wall": wall, "note": tr.spread_note(ph, span, wall)}
                       for ph, span, wall in t["spread_out"]],
        "incomplete": t["incomplete"],
        "estimated": tr.ESTIMATED_TIMES if t["times_estimated"] else None,
    }


def timing_data(state, phase=None, profile=None, directory=None):
    """``{"directory", "phases": [phase_view], "summary": summary_view,
    "no_profiles", "error"}``; ``error`` says why there is nothing (no
    timing JSON yet)."""
    d = timing_dir(state, directory)
    try:
        phases, no_profiles = tr.directory_data(d, phase or None, profile)
    except (RuntimeError, FileNotFoundError) as e:
        return {"directory": d, "phases": [], "summary": None, "no_profiles": False,
                "error": str(e)}
    return {"directory": d, "phases": [phase_view(ph, s, p) for ph, s, p in phases],
            "summary": summary_view(tr.overall_summary(phases)),
            "no_profiles": tr.NO_PROFILES if no_profiles else None, "error": None}


def compare_view(state, before, after, phase=None):
    a, b = timing_dir(state, before), timing_dir(state, after)
    out = []
    for c in tr.compare_data(a, b, phase or None):
        if c["only_in"]:
            out.append({"phase": c["phase"], "only_in": c["only_in"]})
            continue
        metrics = []
        for label, ka, kb, chg in c["metrics"]:
            lower = LOWER_IS_BETTER.get(label)
            better = None if lower is None or abs(chg) < 1 else (chg < 0) == lower
            fmt = tr.METRIC_FORMATS[label]
            metrics.append({"label": label, "before": fmt.format(ka), "after": fmt.format(kb),
                            "change": chg, "better": better})
        stages = [{"name": n, "before": ha, "after": hb, "change": chg,
                   "better": None if chg is None or abs(chg) < 1 else chg < 0}
                  for n, ha, hb, chg in c["stages"]]
        out.append({"phase": c["phase"], "only_in": None, "metrics": metrics, "stages": stages})
    return {"before": a, "after": b, "phases": out}


@router.get("")
def get_timing(request: Request, phase: str = "", profile: int | None = None, dir: str = ""):
    return timing_data(request.app.state.lux, phase, profile, dir or None)


@router.get("/compare")
def get_compare(request: Request, before: str, after: str, phase: str = ""):
    return compare_view(request.app.state.lux, before, after, phase)
