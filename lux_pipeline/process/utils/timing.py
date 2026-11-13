"""Phase timing for the long-running build scripts.

Answers three questions a wall-clock stopwatch cannot:

*   Where did the time go? Named stages around the expensive calls, reported
    as a share of the phase rather than as raw seconds.
*   Is this CPU bound or waiting? Process CPU time against wall time. Near
    100% means python is the wall; well under means the phase is waiting on
    postgres, the network or the disk.
*   Is it getting worse? Progress lines carry the rate for the last interval
    as well as the average, so a phase that degrades shows it while running
    instead of at the end.

Each slice writes its own JSON summary, so a 24-way run can be aggregated
rather than eyeballed across 24 logs.

Cost is a `perf_counter()` pair per stage. At six stages a record that is
about a microsecond per record -- a couple of seconds across a million-record
phase, or ~0.1% of a forty minute run.

A fourth question needs a profiler: *which Python function* inside a stage.
`PhaseTimer(profile=True)`, or ``LUX_PROFILE=1`` in the environment (what
`lux reconcile|merge|export --profile` sets for its workers), runs cProfile
from the timer's start to its finish and writes the stats as
``profile-<phase>-<slice>.prof`` beside the timing JSON; ``lux timing-report
--profile`` adds them up across slices. cProfile slows python-bound code
down considerably, so the timing numbers of a profiled run are not
comparable with an unprofiled one.

Where the reports go: to the ``lux_pipeline.timing`` logger
(`lux_pipeline.log.timing_logger`), not the run's log. A worker of a run
with a prefix sends it to ``<prefix>/w<n>.timing`` and the driver to
``<prefix>/driver.timing`` (`log.send_timing_to`), so the lines a person follows
in ``lux watch`` or the web UI are not interleaved with stage shares
every minute; the run's log gets one line when a phase finishes. With no
file to go to (``lux interactive``, a test) the logger propagates to
wherever ``lux_pipeline`` logs. A `stream` given to `PhaseTimer` gets the
reports instead.

The JSON (``timing-<phase>-<slice>.json``) and profiles go to the same
run directory (`timing_out_dir`), so each run keeps its own and
``lux timing-report`` reads a run, a build or the latest of each phase
under ``log_dir``; outside a run, to ``log_dir`` itself.
"""

import atexit
import json
import os
import sys
import time

from lux_pipeline.log import logger, timing_logger, timing_run_dir
from lux_pipeline.process.utils import host_cpu
from lux_pipeline.progress import null_progress


class _NullStage:
    """What `timing.stage()` returns when no phase is being timed, so library
    code can be instrumented without depending on a timer existing."""

    __slots__ = ()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


_NULL = _NullStage()
_active = None
# the PhaseTimer whose cProfile is running: python allows one profiler at a
# time, so a new timer stops a previous one that never finished
_profiling = None


def profiling_requested():
    """True when ``LUX_PROFILE`` is set to anything but 0/false/no."""
    return os.getenv("LUX_PROFILE", "").strip().lower() not in ("", "0", "false", "no")


def split_stages(stages):
    """Top-level stages, and the nested ones underneath them.

    A dotted name is a child of its prefix -- `acquire.map` is timed *inside*
    `acquire` -- so adding every stage up double-counts. That is what made a
    phase report **-53.1% unattributed**: the four `acquire.*` stages were
    subtracted from the wall clock a second time, and the remainder went
    negative.

    Children are worth reporting but must not be subtracted, and they are not
    a breakdown of their parent either: `timing.stage()` attributes to
    whichever PhaseTimer is active, so a helper called from two different
    stages -- the acquirer, called both directly and from inside
    reconcile() -- lands its children under a parent that did not contain all
    of them. Callers should say so rather than imply a partition.

    A child whose parent was never timed is promoted to top level, since it is
    then the only attribution there is.

    `stages` maps name -> anything; values come back untouched.
    """
    kids = {}
    for name in stages:
        parent = name.split(".", 1)[0]
        if "." in name and parent in stages:
            kids.setdefault(parent, {})[name] = stages[name]
    nested = {n for group in kids.values() for n in group}
    top = {n: v for n, v in stages.items() if n not in nested}
    return top, kids


def stage(name):
    """Time a stage on this process's current PhaseTimer, if there is one.

    Lets code well below the scripts -- acquirers, mappers -- be instrumented
    without threading a timer through every signature. Costs one global lookup
    when timing is off."""
    t = _active
    return t.stage(name) if t is not None else _NULL


def active():
    return _active


class _Stage:
    """Reused per name so the hot path allocates nothing."""

    __slots__ = ("timer", "name", "_t0")

    def __init__(self, timer, name):
        self.timer = timer
        self.name = name
        self._t0 = 0.0

    def __enter__(self):
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb):
        el = time.perf_counter() - self._t0
        acc = self.timer.stages[self.name]
        acc[0] += 1
        acc[1] += el
        return False


def timing_out_dir(cfgs):
    """Where a phase's ``timing-<phase>-<slice>.json`` and profiles go: the
    run's directory, beside its ``w<n>.timing`` (`log.timing_run_dir`), so
    each run keeps its own; outside a run (no ``--prefix``, a root script)
    ``log_dir``, or ``data_dir`` without one."""
    return timing_run_dir() or getattr(cfgs, "log_dir", None) or getattr(cfgs, "data_dir", None)


class PhaseTimer:
    def __init__(self, phase, slice_n=-1, max_slice=-1, total=None,
                 report_every=None, out_dir=None, stream=None,
                 progress=null_progress, profile=None):
        if report_every is None:
            report_every = float(os.getenv("LUX_TIMING_INTERVAL", 60))
        self.phase = phase
        self.slice_n = slice_n
        self.max_slice = max_slice
        self.total = total
        self.report_every = report_every
        self.out_dir = out_dir
        # None: the reports go to timing_logger (see the module docstring)
        self.stream = stream
        # whoever is listening for progress (see lux_pipeline.progress); the
        # default costs one call per step
        self.progress = progress
        self.stages = {}
        self._stage_objs = {}
        self.count = 0
        self.skipped = 0
        self.t0 = time.perf_counter()
        # the clock time it started, so a report can lay phases on a timeline
        self.started_at = time.time()
        self.cpu0 = time.process_time()
        # postgres's and the machine's cpu, which process_time cannot see
        self.host0 = host_cpu.read_counters()
        self._last_report = self.t0
        self._last_count = 0
        self.marks = []
        self.complete = False
        self._profiler = None
        self.profile_file = None
        progress(phase=phase)
        if total is not None:
            progress(total=total)
        # so library code can find it -- see stage() above
        global _active
        _active = self
        # A phase that is killed or crashes is exactly the one whose timing
        # you wanted, so the snapshot is refreshed on every progress report
        # and again on the way out. Only SIGKILL loses the last interval.
        atexit.register(self._at_exit)
        if profile is None:
            profile = profiling_requested()
        if profile:
            self._start_profile()

    def _say(self, text):
        """One report: to `stream` when the timer has one, else a log
        record per non-blank line on `timing_logger`."""
        if self.stream is not None:
            print(text, file=self.stream)
            return
        for line in text.split("\n"):
            if line.strip():
                timing_logger.info(line.rstrip())

    def _flush(self):
        if self.stream is not None:
            self.stream.flush()

    def _at_exit(self):
        self._stop_profile()
        if not self.complete and self.count:
            self.write()

    # ------------------------------------------------------------- profiling

    def _profile_path(self):
        tag = self.slice_n if self.slice_n is not None and self.slice_n > -1 else "all"
        return os.path.join(self.out_dir, f"profile-{self.phase}-{tag}.prof")

    def _start_profile(self):
        global _profiling
        if not self.out_dir:
            self._say(f"  (not profiling {self._tag()}: no directory to write the profile to)")
            return
        if _profiling is not None:
            _profiling._stop_profile()
        import cProfile
        prof = cProfile.Profile()
        try:
            prof.enable()
        except ValueError as e:
            # another profiler is already running in this process
            self._say(f"  (not profiling {self._tag()}: {e})")
            return
        self._profiler = prof
        _profiling = self

    def _stop_profile(self):
        """Stop the profiler, if this timer runs one, and write its stats.
        Like the timing JSON, a failure here never kills the phase."""
        global _profiling
        prof, self._profiler = self._profiler, None
        if prof is None:
            return None
        if _profiling is self:
            _profiling = None
        try:
            prof.disable()
            os.makedirs(self.out_dir, exist_ok=True)
            fn = self._profile_path()
            tmp = f"{fn}.{os.getpid()}.tmp"
            prof.dump_stats(tmp)
            os.replace(tmp, fn)
            self.profile_file = fn
            return fn
        except Exception as e:
            self._say(f"  (could not write profile: {e})")
            return None

    # ------------------------------------------------------------- measuring

    def stage(self, name):
        """`with timer.stage("reidentify"):` around a call worth attributing."""
        obj = self._stage_objs.get(name)
        if obj is None:
            self.stages[name] = [0, 0.0]
            obj = self._stage_objs[name] = _Stage(self, name)
        return obj

    def add(self, name, seconds, calls=1):
        """Record time measured some other way."""
        acc = self.stages.setdefault(name, [0, 0.0])
        acc[0] += calls
        acc[1] += seconds

    def step(self, n=1):
        """One unit of work finished. Prints progress on the interval."""
        self.count += n
        self.progress(advance=n)
        if time.perf_counter() - self._last_report >= self.report_every:
            self.report()

    def skip(self, n=1):
        """Counted, but not work -- records another worker owns, resumed rows."""
        self.skipped += n
        self.progress(advance=n)

    def mark(self, label):
        """Note a boundary within the phase, e.g. moving to the next source."""
        self.marks.append((label, round(time.perf_counter() - self.t0, 1)))
        self.progress(description=label)

    # ------------------------------------------------------------- reporting

    @property
    def elapsed(self):
        return time.perf_counter() - self.t0

    @property
    def cpu(self):
        return time.process_time() - self.cpu0

    def _tag(self):
        if self.slice_n is not None and self.slice_n > -1:
            return f"{self.phase}[{self.slice_n}/{self.max_slice}]"
        return self.phase

    def report(self):
        now = time.perf_counter()
        el = now - self.t0
        window = now - self._last_report
        done = self.count - self._last_count
        rate = self.count / el if el else 0
        wrate = done / window if window else 0
        line = (f"[{self._tag()}] {self.count:,} in {el / 60:.1f}m  "
                f"{rate:,.0f}/s avg  {wrate:,.0f}/s now  "
                f"cpu {self.cpu / el * 100 if el else 0:.0f}%")
        if self.total:
            left = (self.total - self.count) / rate if rate else 0
            line += f"  {self.count / self.total * 100:.0f}% done, ~{left / 60:.0f}m left"
        if self.skipped:
            line += f"  ({self.skipped:,} skipped)"
        self._say(line)
        parents, _kids = split_stages(self.stages)
        top = sorted(parents.items(), key=lambda kv: -kv[1][1])[:4]
        if top and el:
            bits = "  ".join(f"{n} {s[1] / el * 100:.0f}%" for n, s in top)
            self._say(f"[{self._tag()}]   {bits}")
        self._flush()
        self._last_report = now
        self._last_count = self.count
        # keep the on-disk snapshot current, not just the log
        self.write()

    def summary(self):
        el = self.elapsed
        cpu = self.cpu
        rows = sorted(self.stages.items(), key=lambda kv: -kv[1][1])
        # nested stages run inside their parent, so counting them here would
        # subtract the same time twice -- see split_stages()
        top, _kids = split_stages(self.stages)
        accounted = sum(s[1] for s in top.values())
        out = {
            "phase": self.phase,
            "slice": self.slice_n,
            "max_slice": self.max_slice,
            "records": self.count,
            "skipped": self.skipped,
            "seconds": round(el, 1),
            "records_per_second": round(self.count / el, 1) if el else 0,
            "cpu_seconds": round(cpu, 1),
            "cpu_percent": round(cpu / el * 100, 1) if el else 0,
            # `nested` marks a stage that is already inside another, so a
            # reader adding the column up knows to skip it
            "stages": {n: {"calls": s[0], "seconds": round(s[1], 1),
                           "percent": round(s[1] / el * 100, 1) if el else 0,
                           "us_per_call": round(s[1] / s[0] * 1e6, 1) if s[0] else 0,
                           "nested": n not in top}
                       for n, s in rows},
            "unaccounted_seconds": round(el - accounted, 1),
            "marks": self.marks,
            # clock times (epoch seconds): when the phase started, and when this
            # snapshot was taken -- its end, once complete is True
            "started_at": round(self.started_at, 1),
            "updated_at": round(time.time(), 1),
            # False means this is a snapshot of a phase still running (or one
            # that died); the numbers are real but partial
            "complete": self.complete,
            # the cProfile stats beside this file, once written
            "profile": os.path.basename(self.profile_file) if self.profile_file else None,
            # machine-wide counters at the start and now (see host_cpu): a
            # report subtracts the phase's earliest from its latest
            "host_cpu": {"start": self.host0, "now": host_cpu.read_counters()},
            "written_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        try:
            import resource
            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            # linux reports kB, macOS bytes
            out["max_rss_mb"] = round(rss / (1024 if sys.platform == "linux" else 1024 ** 2))
        except Exception:
            pass
        return out

    def finish(self):
        # before the summary, so the profile is the phase and not its report
        self._stop_profile()
        self.complete = True
        self.progress(completed=self.count + self.skipped, done=True)
        s = self.summary()
        el = s["seconds"]
        # the one line of it in the run's log: the rest is timing_logger's
        logger.info(f"{self._tag()} finished: {s['records']:,} records in {el / 60:.1f} min "
                    f"({s['records_per_second']:,.0f}/s)"
                    + (f", {s['skipped']:,} skipped" if s["skipped"] else ""))
        self._say(f"\n=== {self._tag()} finished")
        self._say(f"  {s['records']:,} records in {el / 60:.1f} min "
              f"({s['records_per_second']:,.0f}/s)"
              + (f", {s['skipped']:,} skipped" if s["skipped"] else ""))
        self._say(f"  cpu {s['cpu_seconds'] / 60:.1f} min of {el / 60:.1f} min wall "
              f"= {s['cpu_percent']:.0f}%"
              + ("  (cpu bound)" if s["cpu_percent"] > 85 else
                 "  (waiting on something -- io, postgres, network)"))
        if s.get("max_rss_mb"):
            self._say(f"  peak rss {s['max_rss_mb']:,} MB")
        if s["stages"]:
            top, kids = split_stages(self.stages)
            self._say(f"  {'stage':<20} {'seconds':>9} {'% wall':>7} {'calls':>12} {'us/call':>10}")
            for name, st in s["stages"].items():
                if name not in top:
                    continue
                self._say(f"  {name:<20} {st['seconds']:>9,.1f} {st['percent']:>6.1f}% "
                      f"{st['calls']:>12,} {st['us_per_call']:>10,.1f}")
            self._say(f"  {'(unattributed)':<20} {s['unaccounted_seconds']:>9,.1f} "
                  f"{s['unaccounted_seconds'] / el * 100 if el else 0:>6.1f}%")
            for parent, group in kids.items():
                ptotal = self.stages[parent][1]
                ctotal = sum(v[1] for v in group.values())
                self._say(f"\n  inside {parent} -- already counted above, not additional:")
                for name in sorted(group, key=lambda n: -self.stages[n][1]):
                    st = s["stages"][name]
                    self._say(f"    {name:<18} {st['seconds']:>9,.1f} {'':>7} "
                          f"{st['calls']:>12,} {st['us_per_call']:>10,.1f}")
                if ctotal > ptotal * 1.001:
                    self._say(f"    ...totalling {ctotal:,.1f}s against {parent}'s "
                          f"{ptotal:,.1f}s, so {ctotal - ptotal:,.1f}s of it runs "
                          f"inside other stages -- these are not a breakdown of "
                          f"{parent}")
        # one greppable line per slice, for eyeballing 24 logs at once
        self._say(f"TIMING {self.phase} slice={self.slice_n} records={s['records']} "
              f"seconds={el} rps={s['records_per_second']} cpu_pct={s['cpu_percent']}")
        self._flush()
        self.write()
        return s

    def write(self):
        """Per-slice JSON, so a 24-way run can be added up instead of read.

        Rewritten on every progress report, not only at the end: a phase you
        kill because it is slow is the one whose numbers you most wanted.
        Written to a temporary name and renamed, so a reader watching the file
        during a build never sees half of one."""
        if not self.out_dir:
            return None
        try:
            os.makedirs(self.out_dir, exist_ok=True)
            tag = self.slice_n if self.slice_n is not None and self.slice_n > -1 else "all"
            fn = os.path.join(self.out_dir, f"timing-{self.phase}-{tag}.json")
            tmp = f"{fn}.{os.getpid()}.tmp"
            with open(tmp, "w") as fh:
                json.dump(self.summary(), fh, indent=2)
            os.replace(tmp, fn)
            return fn
        except Exception as e:
            # timing must never be the thing that kills a phase
            self._say(f"  (could not write timing json: {e})")
            return None
