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
"""

import atexit
import json
import os
import sys
import time


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


class PhaseTimer:
    def __init__(self, phase, slice_n=-1, max_slice=-1, total=None,
                 report_every=None, out_dir=None, stream=None):
        if report_every is None:
            report_every = float(os.getenv("LUX_TIMING_INTERVAL", 60))
        self.phase = phase
        self.slice_n = slice_n
        self.max_slice = max_slice
        self.total = total
        self.report_every = report_every
        self.out_dir = out_dir
        self.stream = stream or sys.stdout
        self.stages = {}
        self._stage_objs = {}
        self.count = 0
        self.skipped = 0
        self.t0 = time.perf_counter()
        self.cpu0 = time.process_time()
        self._last_report = self.t0
        self._last_count = 0
        self.marks = []
        self.complete = False
        # A phase that is killed or crashes is exactly the one whose timing
        # you wanted, so the snapshot is refreshed on every progress report
        # and again on the way out. Only SIGKILL loses the last interval.
        atexit.register(self._at_exit)

    def _at_exit(self):
        if not self.complete and self.count:
            self.write()

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
        if time.perf_counter() - self._last_report >= self.report_every:
            self.report()

    def skip(self, n=1):
        """Counted, but not work -- records another worker owns, resumed rows."""
        self.skipped += n

    def mark(self, label):
        """Note a boundary within the phase, e.g. moving to the next source."""
        self.marks.append((label, round(time.perf_counter() - self.t0, 1)))

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
        print(line, file=self.stream)
        top = sorted(self.stages.items(), key=lambda kv: -kv[1][1])[:4]
        if top and el:
            bits = "  ".join(f"{n} {s[1] / el * 100:.0f}%" for n, s in top)
            print(f"[{self._tag()}]   {bits}", file=self.stream)
        self.stream.flush()
        self._last_report = now
        self._last_count = self.count
        # keep the on-disk snapshot current, not just the log
        self.write()

    def summary(self):
        el = self.elapsed
        cpu = self.cpu
        rows = sorted(self.stages.items(), key=lambda kv: -kv[1][1])
        accounted = sum(s[1] for _, s in rows)
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
            "stages": {n: {"calls": s[0], "seconds": round(s[1], 1),
                           "percent": round(s[1] / el * 100, 1) if el else 0,
                           "us_per_call": round(s[1] / s[0] * 1e6, 1) if s[0] else 0}
                       for n, s in rows},
            "unaccounted_seconds": round(el - accounted, 1),
            "marks": self.marks,
            # False means this is a snapshot of a phase still running (or one
            # that died); the numbers are real but partial
            "complete": self.complete,
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
        self.complete = True
        s = self.summary()
        el = s["seconds"]
        print(f"\n=== {self._tag()} finished", file=self.stream)
        print(f"  {s['records']:,} records in {el / 60:.1f} min "
              f"({s['records_per_second']:,.0f}/s)"
              + (f", {s['skipped']:,} skipped" if s["skipped"] else ""), file=self.stream)
        print(f"  cpu {s['cpu_seconds'] / 60:.1f} min of {el / 60:.1f} min wall "
              f"= {s['cpu_percent']:.0f}%"
              + ("  (cpu bound)" if s["cpu_percent"] > 85 else
                 "  (waiting on something -- io, postgres, network)"),
              file=self.stream)
        if s.get("max_rss_mb"):
            print(f"  peak rss {s['max_rss_mb']:,} MB", file=self.stream)
        if s["stages"]:
            print(f"  {'stage':<20} {'seconds':>9} {'% wall':>7} {'calls':>12} {'us/call':>10}",
                  file=self.stream)
            for name, st in s["stages"].items():
                print(f"  {name:<20} {st['seconds']:>9,.1f} {st['percent']:>6.1f}% "
                      f"{st['calls']:>12,} {st['us_per_call']:>10,.1f}", file=self.stream)
            print(f"  {'(unattributed)':<20} {s['unaccounted_seconds']:>9,.1f} "
                  f"{s['unaccounted_seconds'] / el * 100 if el else 0:>6.1f}%", file=self.stream)
        # one greppable line per slice, for eyeballing 24 logs at once
        print(f"TIMING {self.phase} slice={self.slice_n} records={s['records']} "
              f"seconds={el} rps={s['records_per_second']} cpu_pct={s['cpu_percent']}",
              file=self.stream)
        self.stream.flush()
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
            print(f"  (could not write timing json: {e})", file=self.stream)
            return None
