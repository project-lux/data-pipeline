"""Suggest postgres settings for this pipeline, sized to the machine it runs on.

    lux pg-tune                    # inspect and print what to change
    lux pg-tune --workers 32       # the parallelism a build will use (default max_workers)
    lux pg-tune --measure-io       # add an fsync/throughput probe
    lux pg-tune --all              # print settings already correct too
    lux pg-tune --no-db            # machine only, don't connect

`analyse` measures the machine and reads the server, `report` turns that
into the lines `lux pg-tune` prints, and `pending_changes` is the short
form `lux testinstall` checks. Formerly pg-tune.py at the repository root.

**This changes nothing.** It prints SQL for a superuser to review and
run. ALTER SYSTEM writes postgresql.auto.conf, which overrides
postgresql.conf, so keep that in mind if the server is configuration-managed.

The recommendations are not generic postgres advice -- they are shaped by what
this pipeline does, which is unusual in three ways that matter:

*   **Everything is rebuildable.** Record caches are caches, the reference
    queues are created and dropped inside a single run, and identity is
    deterministic from the assertion logs. Durability is worth very little
    here, so the WAL settings trade it away. `run-identify.py` is the one
    phase where that is not true; see the notes at the end.
*   **N processes, not N threads, and each opens three connections** -- two
    from PoolManager, plus one shared by the idmap and the two reference
    maps. It is four on a server whose `synchronous_commit` is `on`, because
    the reference queues then need a session of their own to turn it off
    (performance-backlog.md §3.7). The server is read to tell which
    applies. max_connections is a correctness requirement, not a tuning knob.
*   **Point lookups plus one big filtered scan per worker.** The workers
    already saturate the box, so per-query parallelism on top makes things
    worse, not better.
*   **The identity map is the working set.** Merge is read-bound on it --
    12.2% iowait and 58.9% of active statements in `IO/DataFileRead` when
    `shared_buffers` was 25% of RAM and the idmap was slightly larger than
    that (performance-backlog.md §2.2). So shared_buffers here is sized from
    the measured idmap, not from the usual fraction.

Sizes are anchored on measurement rather than formula: the 128 GB development
box in docs/idmap-migration.md, and the 72 GiB / 36 vCPU build server whose
`top` and `pg_stat_activity` samples are in docs/performance-backlog.md.
"""

import math
import os
import platform
import re
import subprocess
import sys
import time

KB = 1024
MB = KB * 1024
GB = MB * 1024

# pg_settings reports memory in these multiples and time in these units
_MEM_UNITS = {"B": 1, "kB": KB, "8kB": 8 * KB, "16kB": 16 * KB, "32kB": 32 * KB,
              "64kB": 64 * KB, "MB": MB, "GB": GB, "TB": GB * 1024}
_TIME_UNITS = {"us": 0.001, "ms": 1, "s": 1000, "min": 60000, "h": 3600000,
               "d": 86400000}


# --------------------------------------------------------------- formatting

def pg_bytes(n):
    """Postgres-style size literal, in the largest unit that stays exact."""
    for unit, size in (("GB", GB), ("MB", MB), ("kB", KB)):
        if n >= size and n % size == 0:
            return f"{n // size}{unit}"
    return f"{max(1, n // MB)}MB"


def parse_bytes(text):
    m = re.match(r"^\s*(\d+)\s*([kKMGT]?B?)\s*$", str(text))
    if not m:
        return None
    n, unit = int(m.group(1)), m.group(2).upper().rstrip("B")
    return n * {"": 1, "K": KB, "M": MB, "G": GB, "T": GB * 1024}[unit]


def clamp(n, lo, hi):
    return max(lo, min(hi, n))


def round_to(n, step):
    return int(math.ceil(n / step) * step)


def floor_gb(n):
    """Down to a round size, so the printed literal is readable.

    A whole GB above 1 GB, a whole 128MB below it -- flooring a 548MB figure
    to the GB would give zero. These are all "about this much" numbers; a
    literal like '53924352kB' is exact and unreadable, and nobody sanity-
    checks what they cannot read."""
    step = GB if n >= GB else 128 * MB
    return max(step, (n // step) * step)


# ------------------------------------------------------------ the machine

def _cgroup_limit(*paths):
    """A container's limit, if one is set and lower than the host's."""
    for p in paths:
        try:
            with open(p) as fh:
                raw = fh.read().strip().split()[0]
        except OSError:
            continue
        if raw in ("max", "-1"):
            return None
        try:
            v = int(raw)
        except ValueError:
            continue
        # cgroup v1 writes a sentinel rather than "max"
        if v > 0 and v < (1 << 62):
            return v
    return None


def detect_cpus():
    logical = os.cpu_count() or 1
    physical = None
    limited_by = None

    if sys.platform == "linux":
        try:
            with open("/proc/cpuinfo") as fh:
                cores = set()
                phys = core = None
                for line in fh:
                    if line.startswith("physical id"):
                        phys = line.split(":")[1].strip()
                    elif line.startswith("core id"):
                        core = line.split(":")[1].strip()
                        cores.add((phys, core))
                if cores:
                    physical = len(cores)
        except OSError:
            pass
        # cgroup v2 "quota period", v1 quota/period
        quota = None
        try:
            with open("/sys/fs/cgroup/cpu.max") as fh:
                parts = fh.read().split()
                if parts[0] != "max":
                    quota = int(parts[0]) / int(parts[1])
        except (OSError, ValueError, IndexError):
            q = _cgroup_limit("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
            p = _cgroup_limit("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
            if q and p:
                quota = q / p
        if quota and quota < logical:
            limited_by = "cgroup"
            logical = max(1, int(quota))
    elif sys.platform == "darwin":
        physical = _sysctl_int("hw.physicalcpu")
        logical = _sysctl_int("hw.logicalcpu") or logical

    return {"logical": logical, "physical": physical, "limited_by": limited_by}


def _sysctl_int(name):
    try:
        out = subprocess.run(["sysctl", "-n", name], capture_output=True,
                             text=True, timeout=5)
        return int(out.stdout.strip())
    except Exception:
        return None


def detect_ram():
    total = None
    limited_by = None
    if sys.platform == "linux":
        try:
            with open("/proc/meminfo") as fh:
                for line in fh:
                    if line.startswith("MemTotal:"):
                        total = int(line.split()[1]) * KB
                        break
        except OSError:
            pass
        cg = _cgroup_limit("/sys/fs/cgroup/memory.max",
                           "/sys/fs/cgroup/memory/memory.limit_in_bytes")
        if cg and total and cg < total:
            total, limited_by = cg, "cgroup"
    elif sys.platform == "darwin":
        total = _sysctl_int("hw.memsize")
    if total is None:
        try:
            total = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
        except (ValueError, OSError):
            pass
    return {"total": total, "limited_by": limited_by}


def detect_storage(path):
    """Rotational or solid state, for random_page_cost and io_concurrency."""
    out = {"kind": "unknown", "device": None, "why": "could not tell"}
    if sys.platform == "darwin":
        out.update(kind="ssd", why="assumed: apple silicon and recent intel "
                                   "macs are all flash")
        return out
    if sys.platform != "linux":
        return out
    try:
        dev = os.stat(path).st_dev
        major, minor = os.major(dev), os.minor(dev)
        name = os.path.basename(os.path.realpath(f"/sys/dev/block/{major}:{minor}"))
    except OSError:
        return out
    out["device"] = name
    # a partition (nvme0n1p1, sda3) hangs off its parent disk
    for candidate in (name, re.sub(r"p?\d+$", "", name)):
        rot = f"/sys/block/{candidate}/queue/rotational"
        if os.path.exists(rot):
            try:
                with open(rot) as fh:
                    spinning = fh.read().strip() == "1"
            except OSError:
                break
            out["device"] = candidate
            if spinning:
                out.update(kind="rotational", why=f"{candidate} reports rotational=1")
            elif candidate.startswith("nvme"):
                out.update(kind="nvme", why=f"{candidate} is nvme, rotational=0")
            else:
                out.update(kind="ssd", why=f"{candidate} reports rotational=0")
            return out
    # device-mapper, LVM, md: the flag lives on the members, not the mapping
    out["why"] = f"{name} is a virtual device; check the underlying disks"
    return out


# -------------------------------------------------------------- the probe

def probe_fsync(path, n=200):
    """Median and p95 fsync latency, which is what a COMMIT waits for.

    Written where the caller points, so run it on the filesystem holding
    pg_wal to get a number that means anything."""
    fn = os.path.join(path, f".pgtune-{os.getpid()}")
    buf = b"\0" * 8192
    lat = []
    fd = None
    try:
        fd = os.open(fn, os.O_CREAT | os.O_WRONLY, 0o600)
        os.write(fd, buf)
        os.fsync(fd)                                   # warm the file up
        for _ in range(n):
            os.lseek(fd, 0, os.SEEK_SET)
            os.write(fd, buf)
            t0 = time.perf_counter()
            os.fsync(fd)
            lat.append((time.perf_counter() - t0) * 1000)
    except OSError as e:
        return {"error": str(e)}
    finally:
        if fd is not None:
            os.close(fd)
        try:
            os.unlink(fn)
        except OSError:
            pass
    lat.sort()
    return {"median_ms": lat[len(lat) // 2], "p95_ms": lat[int(len(lat) * 0.95)]}


def probe_write(path, mb=256):
    """Rough sequential write throughput, including the final flush."""
    fn = os.path.join(path, f".pgtune-w-{os.getpid()}")
    chunk = b"\0" * MB
    fd = None
    try:
        fd = os.open(fn, os.O_CREAT | os.O_WRONLY, 0o600)
        t0 = time.perf_counter()
        for _ in range(mb):
            os.write(fd, chunk)
        os.fsync(fd)
        el = time.perf_counter() - t0
    except OSError as e:
        return {"error": str(e)}
    finally:
        if fd is not None:
            os.close(fd)
        try:
            os.unlink(fn)
        except OSError:
            pass
    return {"mb_per_s": mb / el if el else 0}


# ------------------------------------------------------------- the server

def connect(db):
    """The database the pipeline itself talks to -- `db` is the
    configuration's cache settings (``Config.caches``) -- so the current
    settings compared against are the ones that will actually apply.
    ``(connection, None)`` or ``(None, why not)``."""
    try:
        import psycopg2
    except ImportError:
        return None, "psycopg2 not installed"
    db = db or {}
    kw = {"user": db.get("user") or os.getenv("USER"),
          "dbname": db.get("dbname") or os.getenv("USER")}
    if db.get("host"):
        kw["host"] = db["host"]
        kw["port"] = db.get("port", 5432)
        if db.get("password"):
            kw["password"] = db["password"]
    try:
        return psycopg2.connect(**kw), None
    except Exception as e:
        return None, f"could not connect ({str(e).strip().splitlines()[0]})"


def read_server(conn, idmap_table="idmap"):
    srv = {"settings": {}}
    with conn.cursor() as cur:
        cur.execute("SELECT current_setting('server_version_num')::int")
        srv["version_num"] = cur.fetchone()[0]
        cur.execute("SELECT version()")
        srv["version"] = cur.fetchone()[0].split(" on ")[0]
        cur.execute("SELECT name, setting, unit, context, source FROM pg_settings")
        for name, setting, unit, context, source in cur.fetchall():
            srv["settings"][name] = {"setting": setting, "unit": unit,
                                     "context": context, "source": source}
        try:
            cur.execute("SELECT setting FROM pg_settings WHERE name = 'data_directory'")
            row = cur.fetchone()
            srv["data_dir"] = row[0] if row else None
        except Exception:
            srv["data_dir"] = None
        cur.execute("""SELECT relname, reloptions FROM pg_class
                       WHERE relname IN ('all_refs','done_refs')""")
        srv["ref_tables"] = {r[0]: (r[1] or []) for r in cur.fetchall()}
        try:
            cur.execute("SELECT sum(pg_total_relation_size(oid)) FROM pg_class "
                        "WHERE relkind = 'r'")
            srv["total_bytes"] = cur.fetchone()[0] or 0
        except Exception:
            srv["total_bytes"] = None

        # The identity map is the random-access working set that merge is
        # read-bound on, so shared_buffers is sized from its measured size
        # rather than from a fraction of RAM -- see size_shared_buffers() and
        # docs/performance-backlog.md §2.2. pg_total_relation_size covers the
        # heap, every index on it and any toast, which is the whole of what
        # has to stay resident.
        srv["idmap_rels"], srv["idmap_bytes"] = {}, None
        srv["idmap_indexes"] = []
        srv["idmap_table"] = idmap_table
        try:
            cur.execute("""SELECT c.relname, pg_total_relation_size(c.oid)
                             FROM pg_class c
                            WHERE c.oid IN (to_regclass(%s), to_regclass(%s))""",
                        (idmap_table, f"{idmap_table}_yuid"))
            rels = {r[0]: r[1] for r in cur.fetchall()}
            if rels:
                srv["idmap_rels"] = rels
                srv["idmap_bytes"] = sum(rels.values())
            # read the index names rather than assuming them, so REINDEX and
            # pg_prewarm below name what actually exists
            cur.execute("""SELECT i.indexrelid::regclass::text
                             FROM pg_index i
                            WHERE i.indrelid IN (to_regclass(%s), to_regclass(%s))
                            ORDER BY 1""",
                        (idmap_table, f"{idmap_table}_yuid"))
            srv["idmap_indexes"] = [r[0] for r in cur.fetchall()]
        except Exception:
            pass
    return srv


def current_value(srv, rec):
    """The live value of one setting, normalised for comparison."""
    if not srv:
        return None, None
    s = srv["settings"].get(rec.name)
    if s is None:
        return None, None
    raw, unit = s["setting"], s["unit"]
    if rec.kind == "bytes" and unit in _MEM_UNITS:
        n = int(raw) * _MEM_UNITS[unit]
        return n, pg_bytes(n)
    if rec.kind == "ms" and unit in _TIME_UNITS:
        n = int(float(raw) * _TIME_UNITS[unit])
        return n, f"{raw}{unit}"
    return raw, raw


def wanted_value(rec):
    if rec.kind == "bytes":
        return parse_bytes(rec.value)
    if rec.kind == "ms":
        m = re.match(r"^(\d+)\s*(us|ms|s|min|h|d)?$", rec.value)
        return int(float(m.group(1)) * _TIME_UNITS.get(m.group(2) or "ms", 1)) if m else None
    return rec.value


# ------------------------------------------------------- recommendations

class Rec:
    def __init__(self, name, value, why, kind="int", restart=False, quote=False,
                 defer=False):
        self.name, self.value, self.why = name, value, why
        self.kind, self.restart, self.quote = kind, restart, quote
        # ALTER SYSTEM validates against parameters the server knows about, and
        # an extension's `foo.bar` GUCs do not exist until its library is
        # loaded. Setting one before the restart that loads it does not warn --
        # it fails with `unrecognized configuration parameter`. Deferred
        # statements are printed after the restart notice instead of inline.
        self.defer = defer

    def statement(self):
        v = f"'{self.value}'" if self.quote or self.kind in ("bytes", "ms", "str") else self.value
        return f"ALTER SYSTEM SET {self.name} = {v};"


def size_shared_buffers(ram, srv):
    """shared_buffers, sized from the measured idmap rather than a fraction.

    The usual advice is 25% of RAM, and for this pipeline that was measured
    wrong. Merge -- the longest phase -- is read-bound on the identity map:
    with shared_buffers at 25% the idmap was marginally *larger* than the
    pool, and merge ran at 12.2% iowait with 58.9% of its active statements
    in `IO/DataFileRead` while 35 GB of the machine sat unused. Reconcile,
    which the earlier round of tuning measured, does not look like this at
    all -- it is latency-bound at 1.5% iowait, which is why 25% survived as
    long as it did (docs/performance-backlog.md §2.1 vs §2.2).

    So: 1.75x the idmap's total size. The headroom is not slack. Merge
    streams the whole of `ils_record_cache` through a cursor while writing
    two more caches, and that eviction pressure is what pushes the idmap's
    random-access pages out. Postgres uses a small ring buffer for
    sequential scans of large tables, so the extra pool is not consumed by
    that streaming -- it is claimed preferentially by exactly the random
    probes that were stalling.

    Bounded below by the old 25% (never recommend *less* than the generic
    answer) and above by half of RAM. Half is higher than the 40% usually
    given, deliberately: the standard warning is about double-buffering
    against the OS page cache, and §2.2's measurement is that the page cache
    is being thrashed by the record-cache streaming anyway, so the memory
    does more good held where the ring buffer cannot evict it. It is a bound
    rather than a target -- it binds only when the idmap is large relative to
    the machine. Returns (bytes, why).
    """
    ceiling = floor_gb(min(ram // 2, 64 * GB))
    floor_ = min(max(128 * MB, floor_gb(ram // 4)), ceiling)
    idmap_bytes = (srv or {}).get("idmap_bytes")

    if not idmap_bytes:
        return floor_, (
            "25% of RAM -- a FLOOR, not the sized answer. This workload is "
            "read-bound on the identity map and 25% was measured too small on "
            "the build box (performance-backlog.md §2.2), but the idmap could "
            "not be measured here, so there is nothing to size against. Re-run "
            "against the built database before trusting this number. An LMDB "
            "read tier in front of postgres was measured slower than simply "
            "sizing this (docs/idmap-migration.md)")

    target = round_to(idmap_bytes * 7 // 4, GB)
    value = clamp(target, floor_, ceiling)
    rels = ", ".join(sorted((srv or {}).get("idmap_rels", {})))
    why = (f"1.75x the measured idmap ({idmap_bytes / GB:.1f} GB of heap and "
           f"indexes across {rels}), NOT the usual 25% of RAM "
           f"({pg_bytes(floor_gb(ram // 4))}). Merge is read-bound on the "
           f"idmap: at 25% it ran at 12.2% iowait with 58.9% of active "
           f"statements in IO/DataFileRead, while a third of the machine sat "
           f"free (performance-backlog.md §2.2). The headroom absorbs merge "
           f"streaming the record caches past it; postgres rings-buffers those "
           f"sequential scans, so the extra pool goes to the random probes that "
           f"were stalling. An LMDB read tier in front of postgres was measured "
           f"slower than simply sizing this (docs/idmap-migration.md)")
    if value == ceiling and target > ceiling:
        why += (f". CAPPED at half of RAM -- the idmap wants "
                f"{pg_bytes(target)}, which this machine cannot give it "
                f"without starving the page cache; merge will stay read-bound")
    return value, why


def recommend(m, srv, workers):
    cpus = m["cpus"]["logical"]
    ram = m["ram"]["total"]
    kind = m["storage"]["kind"]
    vnum = (srv or {}).get("version_num", 150000)
    out = []

    # --- connections: a correctness requirement, not a knob ---------------
    # 2 from PoolManager (read/write, and one for server-side cursors) plus 1
    # for the identity map. The reference queues take a fourth only when they
    # need `synchronous_commit = off` as a session setting -- if the server
    # already defaults to off they share the idmap's connection.
    sync = (srv or {}).get("settings", {}).get("synchronous_commit", {}).get("setting")
    per_worker = 3 if sync == "off" else 4
    shared = ("the reference queues share the idmap's connection because the "
              "server already defaults to synchronous_commit off"
              if per_worker == 3 else
              "the reference queues take a fourth for synchronous_commit = off"
              + (" (server default not read; assuming they need it)"
                 if sync is None else f" (server default is '{sync}')"))
    conns = workers * per_worker + 20
    out.append(("Connections", [
        Rec("max_connections", str(max(100, round_to(conns, 25))),
            f"{workers} workers x {per_worker} connections each: 2 from "
            f"PoolManager (read/write, and one for server-side cursors), 1 for "
            f"the identity map, and {shared}. Plus headroom for psql, "
            f"monitoring and autovacuum. Sized for --workers {workers}: a "
            f"build started with more workers than this was sized for dies "
            f"partway through, so re-run this before changing the worker "
            f"count, not after", restart=True),
    ]))

    # --- memory -----------------------------------------------------------
    shared_buffers, sb_why = size_shared_buffers(ram, srv)
    max_conn = max(100, round_to(conns, 25))
    mem = [
        Rec("shared_buffers", pg_bytes(shared_buffers), sb_why,
            kind="bytes", restart=True),
        Rec("effective_cache_size", pg_bytes(floor_gb(ram * 3 // 4)),
            "planner hint, not an allocation: what the OS page cache plus "
            "shared_buffers can hold", kind="bytes"),
        Rec("work_mem", pg_bytes(clamp(ram // 32 // max_conn, 8 * MB, 128 * MB)),
            "per sort/hash node per connection, so it multiplies by "
            f"max_connections ({max_conn}); this workload is point lookups and "
            "filtered scans, which need little", kind="bytes"),
        Rec("maintenance_work_mem", pg_bytes(floor_gb(clamp(ram // 64, 256 * MB, 4 * GB))),
            "VACUUM and CREATE INDEX on tables of this size take repeated "
            "passes at the 64MB default", kind="bytes"),
        Rec("autovacuum_work_mem", pg_bytes(floor_gb(clamp(ram // 128, 256 * MB, GB))),
            "separate from maintenance_work_mem so N autovacuum workers cannot "
            "each take the larger figure", kind="bytes"),
    ]
    out.append(("Memory", mem))

    # --- WAL: durability is worth little here -----------------------------
    max_wal = clamp(ram // 4, 4 * GB, 32 * GB)
    wal = [
        Rec("synchronous_commit", "off",
            "COMMIT stops waiting for the WAL flush. Everything reconcile and "
            "merge write is reconstructible; a crash loses ~0.6s of commits. "
            "See the note about run-identify.py below", quote=True),
        Rec("max_wal_size", pg_bytes(max_wal),
            "the 1GB default forces constant checkpoints under a build's write "
            "rate", kind="bytes"),
        Rec("min_wal_size", pg_bytes(clamp(max_wal // 16, GB, 4 * GB)),
            "keep recycled segments rather than creating and deleting them",
            kind="bytes"),
        Rec("checkpoint_timeout", "30min",
            "fewer, larger checkpoints; full-page writes after a checkpoint are "
            "a big share of WAL volume", kind="ms", quote=True),
        Rec("checkpoint_completion_target", "0.9",
            "spread the checkpoint's writes over the interval instead of "
            "spiking"),
        Rec("wal_buffers", "64MB",
            f"the -1 default caps at 16MB, which is small for {workers} writers",
            kind="bytes"),
        Rec("wal_compression", "zstd" if vnum >= 150000 else "on",
            "trades CPU for WAL volume; these documents compress well"
            + (" (zstd needs PG15+)" if vnum >= 150000 else ""), quote=True),
    ]
    out.append(("WAL and checkpoints", wal))

    # --- planner and IO ---------------------------------------------------
    io = {"nvme": ("1.0", 300), "ssd": ("1.1", 200),
          "rotational": ("4.0", 2), "unknown": ("1.1", 200)}[kind]
    planner = [
        Rec("random_page_cost", io[0],
            f"storage looks like {kind} ({m['storage']['why']}); the 4.0 "
            "default describes a disk that has to seek"),
        Rec("effective_io_concurrency", str(io[1]),
            "how many concurrent reads the storage can absorb"),
    ]
    if vnum >= 130000:
        planner.append(Rec("maintenance_io_concurrency", str(io[1]),
                           "same, for VACUUM and friends"))
    out.append(("Planner and storage", planner))

    # --- parallelism: less is more with N worker processes ----------------
    out.append(("Parallelism", [
        Rec("max_parallel_workers_per_gather", "0",
            f"DURING A BUILD: {workers} worker processes already saturate "
            f"{cpus} vCPUs, and per-query parallelism on top oversubscribes "
            "them. Raise it for ad-hoc analysis afterwards"),
        Rec("max_worker_processes", str(cpus),
            "the pool the ones below are drawn from", restart=True),
        Rec("max_parallel_workers", str(max(2, cpus // 2)),
            "available to parallel query when you do enable it"),
        Rec("max_parallel_maintenance_workers", str(clamp(cpus // 4, 2, 4)),
            "parallel index builds during VACUUM; PooledCache._maintenance "
            "already asks for 4"),
    ]))

    # --- autovacuum: the reference queues live and die on this ------------
    out.append(("Autovacuum", [
        Rec("autovacuum_max_workers", str(clamp(cpus // 4, 3, 8)),
            "many tables churning at once", restart=True),
        Rec("autovacuum_naptime", "10s",
            "the reference queue is insert-then-delete, so it accumulates dead "
            "tuples in seconds, not minutes; a claim on a bloated queue was "
            "measured at 1.1ms against 0.1ms vacuumed", kind="ms", quote=True),
        Rec("autovacuum_vacuum_cost_delay", "0",
            "do not throttle: there is IO budget, and falling behind on the "
            "reference queue costs far more than the vacuum does", kind="ms",
            quote=True),
    ]))

    # --- observability: the open questions in the backlog need these ------
    # if the library is loaded its GUCs are in pg_settings; if it is not, they
    # do not exist and ALTER SYSTEM rejects them. With no server to ask, assume
    # the worse case -- deferring a statement that would have worked costs a
    # line of output, running one that cannot work costs an error.
    pgss_live = bool(srv) and "pg_stat_statements.track" in srv["settings"]
    obs = [
        Rec("pg_stat_statements.track", "all",
            "so the roll-up sees statements inside functions too", quote=True,
            defer=not pgss_live),
        Rec("pg_stat_statements.max", "10000",
            "this build issues a lot of distinct statement shapes; note this "
            "one is set at server start, so it needs its own restart to take "
            "effect -- the default 5000 is survivable if you would rather not",
            restart=True, defer=not pgss_live),
        Rec("track_io_timing", "on",
            "turns pg_stat_statements into something that can answer 'is this "
            "waiting on IO or on CPU' -- the open question in "
            "docs/performance-backlog.md", quote=True),
        Rec("log_lock_waits", "on",
            "one line per lock wait over deadlock_timeout; the batched writes "
            "and the reference queue both depend on locks staying brief",
            quote=True),
        Rec("log_autovacuum_min_duration", "0",
            "every autovacuum, so you can see whether it keeps up with the "
            "reference queue", kind="ms", quote=True),
    ]
    # shared_preload_libraries has to be appended to, never replaced
    cur = ""
    if srv:
        cur = srv["settings"].get("shared_preload_libraries", {}).get("setting", "")
    libs = [x.strip() for x in cur.split(",") if x.strip()]
    if "pg_stat_statements" not in libs:
        libs.append("pg_stat_statements")
        obs.insert(0, Rec("shared_preload_libraries", ",".join(libs),
                          "appended to what is already loaded -- replacing this "
                          "list would silently unload the rest", quote=True,
                          restart=True))
    out.append(("Observability", obs))
    return out


# ------------------------------------------------------------------ output

def is_current(srv, rec):
    """``(already set as recommended, the current value as text or None)``.
    Never current without a server to compare against."""
    cur_norm, cur_txt = current_value(srv, rec)
    want = wanted_value(rec)
    same = srv is not None and cur_norm is not None and (
        str(cur_norm) == str(want)
        or (rec.kind in ("bytes", "ms") and cur_norm == want))
    return same, cur_txt


def pending_changes(m, srv, workers):
    """The settings the server does not have as recommended for `workers`:
    ``[(name, current text, recommended as ALTER SYSTEM writes it), ...]``,
    in `report`'s order
    (pg_stat_statements' deferred ones included). Needs a server and a
    memory figure; ``None`` without them."""
    if srv is None or not m["ram"]["total"]:
        return None
    out = []
    for _title, recs in recommend(m, srv, workers):
        for rec in recs:
            same, cur_txt = is_current(srv, rec)
            if not same:
                out.append((rec.name, cur_txt, rec.value))
    return out


def analyse(db, no_db=False, idmap_table="idmap", unreadable=None):
    """Measure the machine and, unless `no_db`, read the server the
    pipeline's cache settings `db` point at: ``(machine, server or None,
    why the server was not read)``. `unreadable` says why there are no
    settings, for a configuration that could not be loaded."""
    m = {"cpus": detect_cpus(), "ram": detect_ram()}

    if no_db or db is None:
        srv, why = None, "skipped (--no-db)" if no_db else (unreadable or "no configuration")
    else:
        srv, why = connect(db)
    conn = srv
    srv = None
    if conn is not None:
        try:
            srv = read_server(conn, idmap_table)
        except Exception as e:
            why = f"connected, but could not read pg_settings ({e})"
        finally:
            conn.close()

    data_dir = (srv or {}).get("data_dir")
    m["storage"] = detect_storage(data_dir if data_dir and os.path.isdir(data_dir)
                                  else os.getcwd())

    return m, srv, why


def report(m, srv, why, args):
    """What to change, as ``(lines, status)``: status 1 when nothing could
    be sized (no memory figure). `args` carries ``workers``,
    ``measure_io``, ``io_path``, ``all`` and ``idmap_table``."""
    data_dir = (srv or {}).get("data_dir")
    lines = []

    def out(*parts):
        lines.append(" ".join(str(p) for p in parts) if parts else "")

    # ---- what we found
    out("=" * 78)
    out("machine")
    out("=" * 78)
    c, r = m["cpus"], m["ram"]
    out(f"  platform     {platform.platform()}")
    out(f"  cpus         {c['logical']} logical"
          + (f", {c['physical']} physical" if c["physical"] else "")
          + (f"  (capped by {c['limited_by']})" if c["limited_by"] else ""))
    out(f"  memory       {r['total'] / GB:.1f} GB"
          + (f"  (capped by {r['limited_by']})" if r["limited_by"] else "")
          if r["total"] else "  memory       unknown")
    out(f"  storage      {m['storage']['kind']} -- {m['storage']['why']}")
    if data_dir:
        out(f"  data dir     {data_dir}")

    if not r["total"]:
        out("\nCannot size anything without a memory figure. Stopping.")
        return lines, 1

    if args.measure_io:
        path = args.io_path or (data_dir if data_dir and os.access(data_dir, os.W_OK)
                                else os.getcwd())
        out(f"\n  probing {path} ...")
        f = probe_fsync(path)
        w = probe_write(path)
        if "error" in f:
            out(f"  fsync        could not measure: {f['error']}")
        else:
            out(f"  fsync        {f['median_ms']:.3f} ms median, "
                  f"{f['p95_ms']:.3f} ms p95")
            if f["median_ms"] > 0.5:
                out(f"               ...slow enough that a COMMIT per write "
                      f"would serialise {args.workers} workers behind "
                      f"WALWriteLock. synchronous_commit = off matters here.")
        if "error" not in w:
            out(f"  seq write    {w['mb_per_s']:,.0f} MB/s")
        if sys.platform == "darwin":
            out("               (macOS fsync() does not flush the drive "
                  "cache; the real number is worse)")
        if path != data_dir:
            out("               (not the data directory -- run with "
                  "--io-path pointing at the pg_wal filesystem for a figure "
                  "that means something)")

    out()
    if srv:
        out(f"server       {srv['version']}")
        if srv.get("total_bytes"):
            out(f"             {srv['total_bytes'] / GB:.1f} GB in ordinary tables")
    else:
        out(f"server       not inspected: {why}")
        out("             values below are sized from the machine only, and "
              "every setting is printed")
        out("             because there is nothing to compare against. "
              "Version-dependent choices")
        out("             (wal_compression, maintenance_io_concurrency) "
              "assume PG15 or later.")

    # ---- the recommendations
    groups = recommend(m, srv, args.workers)
    restart_needed, printed, deferred = [], 0, []

    for title, recs in groups:
        rows = []
        for rec in recs:
            same, cur_txt = is_current(srv, rec)
            if same and not args.all:
                continue
            if rec.defer:
                # printing this here would put a statement that cannot succeed
                # in the middle of a block meant to be run top to bottom
                deferred.append(rec)
                continue
            rows.append((rec, cur_txt, same))
        if not rows:
            continue
        out(f"\n{'-' * 78}\n-- {title}\n{'-' * 78}")
        for rec, cur_txt, same in rows:
            for chunk in _wrap(rec.why, 74):
                out(f"-- {chunk}")
            if cur_txt is not None:
                out(f"--   currently: {cur_txt}"
                      + ("   (already correct)" if same else ""))
            out(rec.statement())
            printed += 1
            if rec.restart and not same:
                restart_needed.append(rec.name)
        out()

    if printed == 0 and not deferred:
        out("\nNothing to change -- every setting already matches.\n")
    elif printed:
        out(f"{'-' * 78}")
        out("SELECT pg_reload_conf();")
        if restart_needed:
            out(f"\n-- These need a full restart, not a reload:")
            for n in restart_needed:
                out(f"--   {n}")
            out("-- pg_ctl restart, or systemctl restart postgresql")

    if deferred:
        out(f"\n{'-' * 78}")
        out("-- AFTER that restart, not before")
        out(f"{'-' * 78}")
        out("-- These belong to pg_stat_statements, and its parameters do not")
        out("-- exist until shared_preload_libraries has loaded the library.")
        out("-- Run before the restart, ALTER SYSTEM rejects them outright:")
        out("--   ERROR: unrecognized configuration parameter")
        out("--     \"pg_stat_statements.track\"")
        for rec in deferred:
            out()
            for chunk in _wrap(rec.why, 74):
                out(f"-- {chunk}")
            out(rec.statement())
        out("\nSELECT pg_reload_conf();")

    _tail(out, srv, m, args.idmap_table)
    return lines, 0


def _wrap(text, width):
    out, line = [], ""
    for word in text.split():
        if len(line) + len(word) + 1 > width:
            out.append(line)
            line = word
        else:
            line = f"{line} {word}".strip()
    if line:
        out.append(line)
    return out


def _tail(out, srv, m, idmap_table):
    out(f"\n{'=' * 78}")
    out("not ALTER SYSTEM -- run these too")
    out("=" * 78)

    ref = (srv or {}).get("ref_tables", {})
    need = [t for t, opts in ref.items()
            if not any(o.startswith("autovacuum_vacuum_scale_factor") for o in opts)]
    if srv and not ref:
        out("\n-- all_refs / done_refs do not exist yet; they are created with the")
        out("-- right storage parameters (see REF_STORAGE in")
        out("-- lux_pipeline/storage/idmap/postgres.py). Nothing to do.")
    elif need or not srv:
        out("\n-- The reference queues are work queues, not tables: every claim")
        out("-- deletes a row, so they bloat in seconds and the claim is a LIMIT")
        out("-- scan that walks the corpse pile. REF_STORAGE applies these at")
        out("-- CREATE; tables that already exist need this once.")
        for t in (need or ["all_refs", "done_refs"]):
            out(f"ALTER TABLE {t} SET (fillfactor = 70, autovacuum_enabled = true,")
            out(f"    autovacuum_vacuum_scale_factor = 0.02,")
            out(f"    autovacuum_vacuum_threshold = 5000,")
            out(f"    autovacuum_vacuum_cost_delay = 0,")
            out(f"    autovacuum_analyze_scale_factor = 0.05);")
    else:
        out("\n-- all_refs / done_refs already carry their storage parameters.")

    tbl = idmap_table
    live_idx = (srv or {}).get("idmap_indexes") or []
    # connected and pg_class had nothing for it -- as opposed to --no-db, where
    # we simply could not look
    missing = bool(srv) and not (srv or {}).get("idmap_rels")
    idx = live_idx or [f"{tbl}_pkey", f"{tbl}_yuid_idx", f"{tbl}_yuid_pkey",
                       f"{tbl}_yuid_token_idx"]
    out("\n-- Merge is read-bound on the identity map, and these are cheaper")
    out("-- than the shared_buffers increase above (performance-backlog.md")
    out("-- §2.2 gives them in this order because that is the order of cost).")

    out("\n-- 1. REINDEX. Free, and the indexes are bloated: identify used to")
    out("--    rewrite ~45M rows to the values they already held, one dead")
    out("--    tuple each (§3.10 -- fixed, but the bloat it left is still")
    out("--    on disk, and VACUUM never shrinks an index).")
    if missing:
        out(f"--    {tbl} does not exist yet; nothing to reindex.")
    else:
        out(f"REINDEX (VERBOSE) TABLE CONCURRENTLY {tbl};")
        out(f"REINDEX (VERBOSE) TABLE CONCURRENTLY {tbl}_yuid;")
        out("--    CONCURRENTLY needs PG12+ and is slower; drop it if the")
        out("--    build is stopped, which is the usual case for this.")

    out("\n-- 2. Prewarm. Do this at the START OF MERGE, not only after a")
    out("--    restart: merge streams ils_record_cache through a cursor while")
    out("--    writing two more caches, and that evicts the idmap's")
    out("--    random-access pages as the phase runs (§2.2).")
    out("CREATE EXTENSION IF NOT EXISTS pg_prewarm;")
    out("CREATE EXTENSION IF NOT EXISTS pg_stat_statements;")
    if missing:
        out(f"--    {tbl} does not exist yet; nothing to prewarm.")
    else:
        targets = [f"pg_prewarm('{n}')" for n in idx] + [f"pg_prewarm('{tbl}')"]
        out("SELECT " + ",\n       ".join(targets) + ";")
        if not live_idx:
            out(f"--    (index names assumed -- {tbl} was not read from")
            out("--     pg_index; check pg_indexes if any of these error)")
    out("--    Verify it took: shared_buffers is only worth raising if")
    out("--    IO/DataFileRead falls. Sample pg_stat_activity mid-merge.")

    if sys.platform == "linux" and m["ram"]["total"]:
        # sized from the shared_buffers actually recommended above, which is
        # no longer 25% of RAM -- getting this wrong leaves postgres unable to
        # take the huge pages and silently falling back
        sb, _ = size_shared_buffers(m["ram"]["total"], srv)
        pages = int(sb * 1.1) // (2 * MB)
        out(f"\n-- Huge pages cut the page-table overhead of a large "
              f"shared_buffers across")
        out(f"-- {os.cpu_count()}+ backends. As root, then restart postgres:")
        out(f"--   sysctl -w vm.nr_hugepages={pages}   "
              f"(and add it to /etc/sysctl.conf)")
        out(f"--   ALTER SYSTEM SET huge_pages = 'try';")

    out(f"\n{'=' * 78}")
    out("deliberately NOT recommended")
    out("=" * 78)
    out("""
  full_page_writes = off
      Only safe on storage that guarantees atomic 8kB writes. A torn page
      after a crash is silent corruption, and the WAL saving is not worth it.

  idle_in_transaction_session_timeout, statement_timeout
      A build holds server-side cursors open for the whole of a source --
      over an hour for ILS. Either of these would kill merge and reconcile
      mid-phase. Leave them at 0 for the pipeline's role.

  commit_delay / commit_siblings
      They group WAL flushes that backends are waiting on. With
      synchronous_commit = off nobody is waiting, so they do nothing. Worth
      setting only if you turn synchronous commit back on.

  synchronous_commit = off, around run-identify.py
      That phase writes identity, and unlike the others it is not resumable
      after a crash -- it is deterministic from the assertion files, so the
      fix is to re-run it from the start. If you would rather not carry that,
      SET synchronous_commit = on for its session.
""")
