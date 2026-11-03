"""Machine-wide CPU counters: postgres's, and the host's busy / iowait split.

`time.process_time()`, which `PhaseTimer` reports, is the CPU of the worker's
own Python process. The queries it sends are executed by postgres backends,
separate processes whose CPU it never sees, and a phase waiting on postgres
looks idle from inside the worker. These counters, read at a phase's start
and at every snapshot, let a report say what postgres and the machine as a
whole did over the same window.

`read_counters` returns absolute, monotonically increasing readings, not
rates: every slice of a phase reads the same machine, so a report takes the
earliest reading of the phase and the latest, and subtracts (`delta`) --
adding up per-slice differences would count postgres once per slice.

* ``postgres``: CPU seconds (user + system) of every process called
  ``postgres``, plus the postmaster's reaped-children time (``cutime`` /
  ``cstime``), so a backend that has exited still counts. Only postgres on
  this machine: with the database on another host this is None (or another,
  unrelated local cluster's -- the report cannot tell).
* ``host``: seconds of CPU time across all cpus from ``/proc/stat``, by
  state: ``busy`` (user, nice, system, irq, softirq, steal), ``iowait`` and
  ``idle``; and ``cpus``.

On Linux both are read from ``/proc``. Elsewhere (macOS) they come from
psutil, a dependency on those platforms only, with two differences: a
process's reaped children are not visible, so only live postgres processes
count (a backend that exited during the phase loses its time), and there is
no iowait (``iowait`` is None, and ``busy`` is everything but ``idle``).
Without either, or if nothing can be read, the value is None. Postgres
processes another user owns are visible in ``/proc``; psutil on macOS can
only read one's own (Postgres.app runs as the user). Reading walks every
process: a few milliseconds, done once per progress report
(`LUX_TIMING_INTERVAL`, a minute by default).
"""

import os
import time

PROC = "/proc"


def _ticks():
    try:
        return os.sysconf("SC_CLK_TCK")
    except (AttributeError, ValueError, OSError):
        return 100


def _stat_fields(text):
    """``(comm, fields after the comm)`` from a ``/proc/<pid>/stat`` line.
    The comm is in parentheses and may itself contain spaces or ``)``."""
    lp, rp = text.index("("), text.rindex(")")
    return text[lp + 1:rp], text[rp + 2:].split()


def _psutil():
    try:
        import psutil
        return psutil
    except ImportError:
        return None


# Walking every process costs ~25ms on macOS (a few on Linux): fine once a
# minute, but not for timers created back to back, so a reading is reused
# for this long -- far below the windows it measures.
CACHE_SECONDS = 2.0
_cache = {}


def postgres_seconds(proc=PROC, ticks=None):
    """CPU seconds of the local postgres processes, or None if there are
    none (or nothing to read them with). Reused for `CACHE_SECONDS`."""
    now = time.monotonic()
    hit = _cache.get(proc)
    if hit is not None and now - hit[0] < CACHE_SECONDS:
        return hit[1]
    value = _postgres_seconds(proc, ticks)
    _cache[proc] = (now, value)
    return value


def _postgres_seconds(proc, ticks):
    if not os.path.isdir(proc):
        return _postgres_seconds_psutil()
    ticks = ticks or _ticks()
    try:
        pids = [p for p in os.listdir(proc) if p.isdigit()]
    except OSError:
        return None
    total = 0
    found = False
    for pid in pids:
        try:
            with open(os.path.join(proc, pid, "stat")) as fh:
                comm, rest = _stat_fields(fh.read())
        except (OSError, ValueError):
            continue        # gone between the listing and the read, or not ours to read
        if comm != "postgres":
            continue
        # after the comm: state(0) ... utime(11) stime(12) cutime(13) cstime(14)
        try:
            total += sum(int(rest[i]) for i in (11, 12, 13, 14))
        except (IndexError, ValueError):
            continue
        found = True
    return total / ticks if found else None


def _postgres_seconds_psutil():
    psutil = _psutil()
    if psutil is None:
        return None
    total = 0.0
    found = False
    try:
        # names first: asking every process for its cpu times costs ~30ms
        for p in psutil.process_iter(["name"]):
            if p.info.get("name") != "postgres":
                continue
            try:
                ct = p.cpu_times()
            except Exception:
                continue        # exited meanwhile, or another user's (access denied)
            total += (ct.user + ct.system + getattr(ct, "children_user", 0.0)
                      + getattr(ct, "children_system", 0.0))
            found = True
    except Exception:
        return None
    return total if found else None


def host_seconds(proc=PROC, ticks=None):
    """``{"busy", "iowait", "idle", "cpus"}`` in CPU seconds summed across
    cpus (``/proc/stat``, or psutil), or None. ``iowait`` is None where the
    platform has none."""
    if not os.path.isdir(proc):
        return _host_seconds_psutil()
    ticks = ticks or _ticks()
    try:
        with open(os.path.join(proc, "stat")) as fh:
            lines = fh.read().splitlines()
    except OSError:
        return None
    try:
        vals = [int(v) for v in lines[0].split()[1:]]
    except (IndexError, ValueError):
        return None
    # user nice system idle iowait irq softirq steal [guest guest_nice]
    vals += [0] * (8 - len(vals))
    user, nice, system, idle, iowait, irq, softirq, steal = vals[:8]
    cpus = sum(1 for ln in lines[1:] if ln.startswith("cpu") and ln[3:4].isdigit())
    return {"busy": (user + nice + system + irq + softirq + steal) / ticks,
            "iowait": iowait / ticks, "idle": idle / ticks, "cpus": cpus or None}


def _host_seconds_psutil():
    psutil = _psutil()
    if psutil is None:
        return None
    try:
        ct = psutil.cpu_times()
        fields = ct._asdict()
        idle = fields.pop("idle")
        iowait = fields.pop("iowait", None)
        # guest time is already counted in user on linux; macOS has none
        fields.pop("guest", None), fields.pop("guest_nice", None)
        return {"busy": sum(fields.values()), "iowait": iowait, "idle": idle,
                "cpus": psutil.cpu_count()}
    except Exception:
        return None


def read_counters(proc=PROC):
    """The readings now: ``{"at": epoch seconds, "postgres": ..., "host": ...}``."""
    ticks = _ticks()
    return {"at": round(time.time(), 1),
            "postgres": _round(postgres_seconds(proc, ticks)),
            "host": _round_dict(host_seconds(proc, ticks))}


def _round(v):
    return None if v is None else round(v, 2)


def _round_dict(d):
    return None if d is None else {k: (round(v, 2) if isinstance(v, float) else v)
                                   for k, v in d.items()}


def delta(first, last):
    """What happened between two `read_counters` readings: ``{"seconds",
    "postgres" (cpu seconds or None), "postgres_cores", "busy_percent",
    "iowait_percent", "cpus"}`` -- or None if they cannot be compared. A
    counter that went backwards (postgres restarted, a reboot) is None."""
    if not first or not last or last["at"] <= first["at"]:
        return None
    secs = last["at"] - first["at"]
    out = {"seconds": secs, "postgres": None, "postgres_cores": None,
           "busy_percent": None, "iowait_percent": None, "cpus": None}
    a, b = first.get("postgres"), last.get("postgres")
    if a is not None and b is not None and b >= a:
        out["postgres"] = b - a
        out["postgres_cores"] = (b - a) / secs
    ha, hb = first.get("host"), last.get("host")
    if ha and hb:
        has_iowait = ha.get("iowait") is not None and hb.get("iowait") is not None
        keys = ("busy", "iowait", "idle") if has_iowait else ("busy", "idle")
        d = {k: hb[k] - ha[k] for k in keys}
        total = sum(d.values())
        if total > 0 and min(d.values()) >= 0:
            out["busy_percent"] = d["busy"] / total * 100
            # None on a platform without iowait (macOS)
            out["iowait_percent"] = d["iowait"] / total * 100 if has_iowait else None
            out["cpus"] = hb.get("cpus")
    if out["postgres"] is None and out["busy_percent"] is None:
        return None
    return out
