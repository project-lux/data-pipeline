"""Load dump files into a datacache.

A source config describes its dump files as ``input_files``: a dict of load
type (``records`` by default) to a list of ``{"path"|"url", "type"}``
entries, where ``type`` is a small path language::

    (container[.compression]/)*format[.compression]

    containers:  dir dirs pair zip tar lines dict array arraylines xmls lmdb
    formats:     json raw other
    compression: gz bz2 z

    dir         a directory of files
    dirs, pair  a directory hierarchy of files (pair: two-character subdirs)
    zip, tar    an archive of members
    lines       a file where each line is a member
    dict        a JSON file whose top level is an object; each value is a member
    array       a JSON file whose top level is an array; each item is a member
    arraylines  a JSON array written one record per line
    xmls        an XML file; each child of the root is a member
    lmdb        an LMDB distribution store (make_*_lmdb.py); each value is a member
    json        a JSON record (a file, a member, or an already parsed item)
    raw         an already parsed JSON item (from dict / array)
    other       anything else, kept as text under ``{"data": ...}``

For example ``tar.gz/lines.bz2/json`` is a gzipped tar whose members are
bzip2'd JSON Lines files; a datacache export is ``zip.bz2/json``; an LMDB
store of zlib-compressed records is ``lmdb/json.z``.

Loading walks the spec: each container step yields children (paths,
archive members, lines, JSON items) to the next step; the format step
builds ``{"identifier", "data"}`` and stores it. Slicing happens at the
format step (``seen % max_slice == my_slice``), so N workers can read the
same file and every worker must see the children in the same order --
which is why directory listings are sorted.

The legacy entry points (``load()``, ``load_export()``, ``load_export2()``
and the ``get_identifier_*`` / ``filter_line`` hooks) are kept at the
bottom for the sources that still override them; ``uses_dsl`` tells the
load manager which path a source takes.
"""

import bz2
import gzip
import inspect
import io
import logging
import os
import tarfile
import time
import zipfile
import zlib

import lmdb
import ujson as json
from lxml import etree

from lux_pipeline.progress import Sliced, null_progress

logger = logging.getLogger("lux_pipeline")

CONTAINERS = ("dir", "dirs", "pair", "zip", "tar", "lines", "dict", "array",
              "arraylines", "xmls", "lmdb")
FORMATS = ("json", "raw", "other")
COMPRESSIONS = ("gz", "bz2", "z")

# files above this are not read through to count their lines for progress
COUNT_LINES_BELOW = 100_000_000


class _StopLoad(Exception):
    """Raised inside the walk when max_records is reached; caught in process()."""


class Pointer:
    """A member of a container that cannot be re-opened by path: carries
    the parent and enough to open the member once."""

    def __init__(self, parent, info):
        self.parent = parent
        self.info = info


class TarPointer(Pointer):
    # parent is a TarFile, info is a TarInfo
    def get_name(self):
        return self.info.name

    def get_handle(self):
        return self.parent.extractfile(self.info)


class ZipPointer(Pointer):
    # parent is a ZipFile, info is the member name
    def get_name(self):
        return self.info

    def get_handle(self):
        return self.parent.open(self.info)


class LmdbPointer(Pointer):
    # parent is an lmdb Environment, info is (key, value) as memoryviews
    def get_name(self):
        key = bytes(self.info[0]).decode("utf-8")
        if ":" in key:
            key = key.split(":", 1)[1]
        return key

    def get_handle(self):
        return self.info[1]


class Loader(Sliced):
    # a subclass may add container step names of its own: list them here
    # and register their iterate_* functions in step_functions (see
    # sources/dnb/loader.py for one)
    extra_containers = ()

    def __init__(self, config):
        self.config = config
        self.in_url = config.get("remoteDumpFile", "")
        self.in_path = config.get("dumpFilePath", "")
        self.out_cache = config["datacache"]
        self.total = config.get("totalRecords", -1)
        self._init_dsl()

    # --- state ---------------------------------------------------------------

    def _init_dsl(self):
        """The DSL's state. Separate from __init__ because many source
        loaders override __init__ without calling it; prepare() calls this
        when it finds the state missing, so those sources still answer
        uses_dsl (False) and the manager falls back to their load()."""
        config = getattr(self, "config", None) or {}
        self.config = config
        self.name = config.get("name", "")
        self.configs = config.get("all_configs")
        self.mapper = config.get("mapper")
        if not hasattr(self, "total"):
            self.total = config.get("totalRecords", -1)
        if not hasattr(self, "out_cache"):
            self.out_cache = config.get("datacache")
        self.dumps_dir = getattr(self.configs, "dumps_dir", "") or ""
        if config.get("dumps_dir"):
            self.dumps_dir = os.path.join(self.dumps_dir, config["dumps_dir"])
        self.seen = 0          # records met at the format step, all slices
        self.handled = 0       # records this slice made
        self.stored = 0        # records this slice stored
        self.running_total = 0
        self.my_files = []
        self.uses_dsl = False
        self.load_type = "records"
        self.temp_file_handles = {}
        self.overwrite = True
        self.max_records = -1
        self.failed_files = []
        self._lmdb_envs = {}
        self._ppj_takes_identifier = None
        self.step_functions = {
            "dir": self.iterate_directory,
            "dirs": self.iterate_directories,
            "pair": self.iterate_directories,
            "zip": self.iterate_zip,
            "tar": self.iterate_tar,
            "lines": self.iterate_lines,
            "arraylines": self.iterate_arraylines,
            "dict": self.iterate_dict,
            "array": self.iterate_array,
            "xmls": self.iterate_xmls,
            "lmdb": self.iterate_lmdb,
            "json": self.make_json,
            "raw": self.make_raw,
            "other": self.make_other,
        }

    def _ensure_dsl(self):
        if not hasattr(self, "step_functions"):
            self._init_dsl()

    # --- the spec ------------------------------------------------------------

    def process_fmt(self, fmt):
        """``"tar.gz/lines/json"`` -> ``[["tar", "gz"], ["lines"], ["json"]]``,
        or ValueError naming what is wrong."""
        spec = []
        bits = fmt.split("/")
        last = bits.pop(-1)
        for b in bits:
            bb = b.split(".")
            if bb[0] not in CONTAINERS and bb[0] not in self.extra_containers:
                raise ValueError(f"Cannot process container type {bb[0]!r} in {self.name} loader")
            if len(bb) == 2 and bb[1] not in COMPRESSIONS:
                raise ValueError(f"Cannot process compression type {bb[1]!r} in {self.name} loader")
            if len(bb) > 2:
                raise ValueError(f"Badly specified container {b!r} in {self.name} loader")
            spec.append(bb)
        fmts = last.split(".")
        if fmts[0] not in FORMATS:
            raise ValueError(f"Cannot process format type {fmts[0]!r} in {self.name} loader")
        if len(fmts) == 2 and fmts[1] not in COMPRESSIONS:
            raise ValueError(f"Cannot process compression type {fmts[1]!r} in {self.name} loader")
        if len(fmts) > 2:
            raise ValueError(f"Badly specified format {last!r} in {self.name} loader")
        spec.append(fmts)
        return spec

    def guess_fmt(self, path):
        """A spec from the file name, for an entry without ``type``. Only
        the common shapes; anything else must say what it is."""
        if os.path.isdir(path):
            for f in sorted(os.listdir(path)):
                full = os.path.join(path, f)
                if os.path.isfile(full):
                    return [["dir"], *self.guess_fmt(full)]
            raise ValueError(f"Cannot guess the type of empty directory {path}; set 'type'")
        comp = None
        base = path
        if base.endswith(".gz"):
            comp, base = "gz", base[:-3]
        elif base.endswith(".bz2"):
            comp, base = "bz2", base[:-4]
        elif base.endswith(".tgz"):
            comp, base = "gz", base[:-4] + ".tar"
        step = lambda name: [name, comp] if comp else [name]
        if base.endswith(".jsonl"):
            return [step("lines"), ["json"]]
        if base.endswith(".json"):
            return [step("json")]
        if base.endswith(".zip"):
            return [["zip"], ["json"]]
        if base.endswith(".tar"):
            return [step("tar"), ["json"]]
        if base.endswith(".xml"):
            return [step("other")]
        raise ValueError(f"Cannot guess the type of {path}; set 'type' in input_files")

    # --- progress ------------------------------------------------------------

    def _per_slice(self, n):
        return n // self.max_slice if self.max_slice > 1 else n

    def _add_total(self, n):
        """A container learned how many children it has; the running total
        is for the whole input, reported as this slice's share."""
        if self.increment_total and n > 0:
            self.running_total += n
            self.progress(total=self._per_slice(self.running_total))

    # --- containers ----------------------------------------------------------

    def iterate_directory(self, path, comp, remaining):
        files = sorted(os.listdir(path))
        if len(remaining) == 1:
            self._add_total(len(files))
        for f in files:
            full = os.path.join(path, f)
            if os.path.isfile(full):
                yield full

    def iterate_directories(self, path, comp, remaining):
        for f in sorted(os.listdir(path)):
            full = os.path.join(path, f)
            if os.path.isfile(full):
                if len(remaining) == 1:
                    self._add_total(1)
                yield full
            elif os.path.isdir(full):
                yield from self.iterate_directories(full, comp, remaining)

    def iterate_zip(self, path, comp, remaining):
        if comp == "bz2":
            compression = zipfile.ZIP_BZIP2
        elif comp == "gz":
            compression = zipfile.ZIP_DEFLATED
        else:
            compression = zipfile.ZIP_STORED
        with zipfile.ZipFile(path, compression=compression) as zh:
            names = [n for n in zh.namelist() if not n.endswith("/")]
            if len(remaining) == 1:
                self._add_total(len(names))
            for n in names:
                yield ZipPointer(zh, n)

    def iterate_tar(self, path, comp, remaining):
        # No count: reading the index of a 100 GB tar just to count it
        # costs as much as the load
        mode = f"r:{comp}" if comp else "r"
        if isinstance(path, Pointer):
            th = tarfile.open(fileobj=path.get_handle(), mode=mode)
        else:
            th = tarfile.open(path, mode)
        with th:
            ti = th.next()
            while ti is not None:
                if ti.isfile():
                    yield TarPointer(th, ti)
                ti = th.next()

    def _lmdb_env(self, path):
        if path not in self._lmdb_envs:
            if not os.path.exists(path):
                raise FileNotFoundError(f"LMDB path not found: {path}")
            self._lmdb_envs[path] = lmdb.open(path, max_dbs=3, readonly=True, lock=False)
        return self._lmdb_envs[path]

    def _lmdb_cursor(self, txn, env):
        db = env.open_db(b"data", dupsort=False, txn=txn)
        return txn.cursor(db=db)

    def lmdb_count_entries(self, path):
        env = self._lmdb_env(path)
        prefix = f"{self.name}:".encode("utf-8")
        count = 0
        with env.begin(buffers=True) as txn:
            cursor = self._lmdb_cursor(txn, env)
            if cursor.set_range(prefix):
                while bytes(cursor.key()).startswith(prefix):
                    count += 1
                    if not cursor.next():
                        break
        return count

    def iterate_lmdb(self, path, comp, remaining):
        if comp:
            raise NotImplementedError("A compressed LMDB container is not supported; "
                                      "compress the format instead, e.g. lmdb/json.z")
        env = self._lmdb_env(path)
        prefix = f"{self.name}:".encode("utf-8")
        with env.begin(buffers=True) as txn:
            cursor = self._lmdb_cursor(txn, env)
            if cursor.set_range(prefix):
                for key, value in cursor:
                    if not bytes(key).startswith(prefix):
                        break
                    yield LmdbPointer(env, (key, value))

    def file_opener(self, path, comp):
        """A binary file object for `path`: a filesystem path, a Pointer
        into an archive or LMDB store, or an already open file object.
        Always binary; the format steps decode."""
        if isinstance(path, LmdbPointer):
            raw = path.get_handle()
            if comp == "z":
                raw = zlib.decompress(raw)
            elif comp == "gz":
                raw = gzip.decompress(raw)
            elif comp == "bz2":
                raw = bz2.decompress(raw)
            return io.BytesIO(bytes(raw))
        if isinstance(path, Pointer):
            path = path.get_handle()
        if isinstance(path, io.IOBase) and not comp:
            return path
        if comp == "gz":
            return gzip.open(path)
        if comp == "bz2":
            return bz2.open(path)
        if comp == "z":
            with open(path, "rb") as fh:
                return io.BytesIO(zlib.decompress(fh.read()))
        return open(path, "rb")

    def count_lines(self, fh):
        """Lines in an open file, or -1 when counting would cost a full
        read of a large or compressed stream."""
        if isinstance(fh, (gzip.GzipFile, bz2.BZ2File)):
            return -1
        try:
            length = fh.seek(0, os.SEEK_END)
            fh.seek(0)
        except (OSError, AttributeError, io.UnsupportedOperation):
            return -1
        if length >= COUNT_LINES_BELOW:
            return -1
        lines = sum(1 for _ in fh)
        fh.seek(0)
        return lines

    def iterate_lines(self, path, comp, remaining):
        with self.file_opener(path, comp) as fh:
            if len(remaining) == 1:
                self._add_total(self.count_lines(fh))
            for line in fh:
                if isinstance(line, str):
                    line = line.encode("utf-8")
                if line.strip():
                    yield io.BytesIO(line)

    def iterate_arraylines(self, path, comp, remaining):
        """A JSON array written one record per line: strip the brackets
        and commas and yield each record."""
        with self.file_opener(path, comp) as fh:
            if len(remaining) == 1:
                self._add_total(self.count_lines(fh))
            for line in fh:
                if isinstance(line, bytes):
                    line = line.decode("utf-8")
                line = line.strip()
                if line and line[0] in "[,":
                    line = line[1:]
                if line and line[-1] in "],":
                    line = line[:-1]
                line = line.strip()
                if len(line) < 2 or line[0] != "{" or line[-1] != "}":
                    continue
                yield io.BytesIO(line.encode("utf-8"))

    def iterate_dict(self, path, comp, remaining):
        with self.file_opener(path, comp) as fh:
            data = json.load(fh)
        if len(remaining) == 1:
            self._add_total(len(data))
        for v in data.values():
            yield v

    def iterate_array(self, path, comp, remaining):
        with self.file_opener(path, comp) as fh:
            data = json.load(fh)
        if len(remaining) == 1:
            self._add_total(len(data))
        for v in data:
            yield v

    def iterate_xmls(self, path, comp, remaining):
        """Children of the root element; the whole file is read into memory."""
        with self.file_opener(path, comp) as fh:
            data = fh.read()
        try:
            dom = etree.XML(data)
        except Exception as e:
            logger.error(f"Error parsing XML {path} for {self.name}: {e}")
            return
        if len(remaining) == 1:
            self._add_total(len(dom))
        for kid in dom.iterchildren():
            yield io.BytesIO(etree.tostring(kid))

    # --- formats -------------------------------------------------------------

    def make_raw(self, item, comp, parent):
        """An already parsed JSON item from dict / array."""
        data = self._post_process_json(item, None)
        if data is None:
            return None
        ident = self.extract_identifier(data)
        if not ident:
            raise ValueError(f"Could not get an identifier in {self.name} while in {parent}")
        return {"identifier": ident, "data": data}

    def make_json(self, path, comp, parent):
        ident = self.make_identifier(path)
        if isinstance(path, dict):
            data = path
        else:
            with self.file_opener(path, comp) as fh:
                data = json.load(fh)
        try:
            data = self._post_process_json(data, ident)
        except Exception:
            logger.debug(f"Failed to post_process {ident} in {self.name}: {data!r:.200}")
            raise
        if data is None:
            return None
        if not ident:
            ident = self.extract_identifier(data)
            if not ident:
                raise ValueError(f"Could not get an identifier in {self.name} while in {parent}/{path}")
        return {"identifier": ident, "data": data}

    def make_other(self, path, comp, parent):
        ident = self.make_identifier(path)
        with self.file_opener(path, comp) as fh:
            data = fh.read()
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        data = self.post_process_other(data)
        if data is None:
            return None
        if not isinstance(data, dict):
            data = {"data": data}
        if not ident:
            ident = self.extract_identifier(data)
            if not ident:
                return None
        return {"identifier": ident, "data": data}

    def make_identifier(self, value):
        """The last path component without its extensions, from a path, a
        Pointer, a named file object or a URI; None when there is none."""
        if isinstance(value, Pointer):
            value = value.get_name()
        elif hasattr(value, "name") and isinstance(getattr(value, "name"), str):
            value = value.name
        elif isinstance(value, bytes):
            value = value.decode("utf-8")
        if not isinstance(value, str):
            return None
        last = value.rstrip("/").split("/")[-1]
        return last.split(".")[0] or None

    def extract_identifier(self, data):
        """The identifier from the record itself; the default looks at ``id``."""
        if isinstance(data, dict) and "id" in data:
            return self.make_identifier(data["id"])
        return None

    def _post_process_json(self, data, identifier):
        """Call post_process_json with one or two arguments, whichever the
        (sub)class defines: main's sources take ``(js)``, wikidata takes
        ``(js, identifier)``. Decided once per instance."""
        if self._ppj_takes_identifier is None:
            params = inspect.signature(self.post_process_json).parameters.values()
            positional = [p for p in params if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
            varargs = any(p.kind == p.VAR_POSITIONAL for p in params)
            self._ppj_takes_identifier = varargs or len(positional) >= 2
        if self._ppj_takes_identifier:
            return self.post_process_json(data, identifier)
        return self.post_process_json(data)

    def post_process_json(self, data, identifier=None):
        """After parsing and before the identifier is extracted; return None
        to drop the record."""
        return data

    def post_process_other(self, data):
        """After reading a non-JSON record and before the identifier is extracted."""
        return data

    # --- the walk --------------------------------------------------------------

    def should_process_item(self, child):
        """Whether a container child goes to the next step; e.g. skip the
        .csv beside the .json in the ROR zip."""
        return True

    def should_make_record(self, path):
        if self.max_slice > 1 and self.seen % self.max_slice != self.my_slice:
            return False
        return True

    def should_store_record(self, record):
        """With overwrite=False, ``identifier in out_cache`` is a round trip
        to the database per record."""
        if record is None or record.get("data") is None or record.get("identifier") is None:
            return False
        if not self.overwrite and record["identifier"] in self.out_cache:
            return False
        return True

    def store_record(self, record):
        try:
            self.out_cache[record["identifier"]] = record["data"]
        except Exception as e:
            logger.error(f"Failed to store {record['identifier']} for {self.name}: {e}")
            return False
        if hasattr(self.out_cache, "checkpoint"):
            self.out_cache.checkpoint()
        self.stored += 1
        self.progress(advance=1)
        return True

    def post_store_record(self, record):
        """Write what the mapper wants indexed to this slice's TSV files."""
        entries = 0
        if self.temp_file_handles and self.mapper is not None:
            fields = self.mapper.extract_index_data(record) or {}
            for f, vals in fields.items():
                fh = self.temp_file_handles.get(f)
                if fh is None:
                    continue
                for a, b in vals:
                    fh.write(f"{a}\t{b}\n")
                    entries += 1
        return entries

    def open_temp_files(self):
        """One TSV per index kind this source is configured for, named for
        the slice, under the temp_dir; the index loader (item 18) reads them."""
        temp_dir = getattr(self.configs, "temp_dir", None)
        if not temp_dir or self.mapper is None or not hasattr(self.mapper, "extract_index_data"):
            return
        kinds = [("reconcileDbPath", "label", "labels"),
                 ("inverseEquivDbPath", "equiv", "equivs"),
                 ("hasDifferentFrom", "diff", "diffs")]
        for key, kind, word in kinds:
            if self.config.get(key):
                fn = os.path.join(temp_dir, f"{self.name}_{word}_{self.my_slice}.tsv")
                self.temp_file_handles[kind] = open(fn, "w")

    def close_temp_files(self):
        for fh in self.temp_file_handles.values():
            if fh and not fh.closed:
                fh.close()
        self.temp_file_handles = {}

    def process_step(self, steps, path, parent):
        step = steps[0]
        comp = step[1] if len(step) > 1 else None
        handler = self.step_functions.get(step[0])
        if handler is None:
            raise ValueError(f"Unknown step type {step} in {self.name}")
        if step[0] in CONTAINERS or step[0] in self.extra_containers:
            for child in handler(path, comp, steps[1:]):
                if self.should_process_item(child):
                    self.process_step(steps[1:], child, step)
        else:
            self.seen += 1
            if self.should_make_record(path):
                self.handled += 1
                record = handler(path, comp, parent)
                if record and self.should_store_record(record):
                    if self.store_record(record):
                        self.post_store_record(record)
                if self.max_records > 0 and self.handled >= self.max_records:
                    raise _StopLoad()

    def prepare(self, my_slice=-1, max_slice=-1, load_type="records", max_records=-1,
                progress=null_progress):
        """Work out which files this loader reads, as ``my_files``:
        ``[{"path", "fmt"}]`` with the spec parsed. ``uses_dsl`` says
        whether the config describes its files at all; when it does not,
        the manager calls the legacy ``load()``."""
        self._ensure_dsl()
        Sliced.prepare(self, my_slice, max_slice, progress)
        self.load_type = load_type
        if max_records != -1:
            self.max_records = max_records
        if load_type == "export":
            self.total = -1

        files = []
        ifs = self.config.get("input_files") or {}
        self.uses_dsl = bool(ifs) or bool(self.config.get("dumpFileType"))
        if ifs:
            if load_type not in ifs:
                logger.error(f"No configured file for load type {load_type!r} in source {self.name}")
            for p in ifs.get(load_type, []):
                path = p.get("path")
                url = p.get("url")
                if path is None and url is None:
                    logger.warning(f"input_files entry without path or url in {self.name}: {p}")
                    continue
                if path is None:
                    path = url.split("/")[-1]
                if not os.path.isabs(path):
                    path = os.path.join(self.dumps_dir, path)
                fmt = p.get("type")
                fmtspec = self.process_fmt(fmt) if fmt else self.guess_fmt(path)
                files.append({"path": path, "fmt": fmtspec})
        if not files and load_type == "records" and (dfp := self.config.get("dumpFilePath")):
            fmt = self.config.get("dumpFileType")
            if fmt:
                files.append({"path": dfp, "fmt": self.process_fmt(fmt)})
            elif self.uses_dsl:
                files.append({"path": dfp, "fmt": self.guess_fmt(dfp)})
        self.my_files = files

        if load_type == "lmdb" and files:
            self.total = self.lmdb_count_entries(files[0]["path"])
        self.increment_total = self.total < 0
        return self

    def process(self, overwrite=True):
        """Load ``my_files``; return how many records this slice stored.
        Commits are deferred and checkpointed per record as load_export2
        does; a file that fails is logged, the rest are still loaded, and
        the failures are raised together at the end so the worker is seen
        to fail."""
        self._ensure_dsl()
        self.overwrite = overwrite
        self.increment_total = self.total < 0
        if self.total > 0:
            self.progress(total=self._per_slice(self.total))
        if not self.my_files:
            logger.warning(f"{self.name}: process() called with no files to load")
            return 0

        cache = self.out_cache
        self.open_temp_files()
        if hasattr(cache, "defer_commits"):
            cache.defer_commits(every=1000)
        try:
            for info in self.my_files:
                path = info["path"]
                self.progress(description=os.path.basename(str(path).rstrip("/")))
                logger.info(f"Loading {path} for {self.name} in slice {self.my_slice}")
                try:
                    self.process_step(info["fmt"], path, None)
                except _StopLoad:
                    logger.info(f"{self.name}: stopping after {self.handled} records (max_records)")
                    break
                except Exception as e:
                    logger.error(f"Failed to load {path} for {self.name}: {e}", exc_info=True)
                    self.failed_files.append(path)
        finally:
            if hasattr(cache, "resume_commits"):
                cache.resume_commits()
            elif hasattr(cache, "commit"):
                cache.commit()
            self.close_temp_files()
            for env in self._lmdb_envs.values():
                env.close()
            self._lmdb_envs = {}
        if self.failed_files:
            raise RuntimeError(f"{self.name}: {len(self.failed_files)} file(s) failed to load: "
                               f"{', '.join(str(f) for f in self.failed_files)}")
        return self.stored

    # --- legacy ----------------------------------------------------------------
    # main's loader, kept verbatim for the sources that still override load()

    def get_identifier_raw(self, line):
        # Find identifier from raw line
        return None

    def get_identifier_json(self, js):
        return None

    def filter_line(self, line):
        return False

    def load(self):
        # default is to assume gzipped JSONL
        # without headers/footers or other wrapping
        # Dump in raw without parsing

        if self.in_path.endswith(".gz"):
            fh = gzip.open(self.in_path)
        elif self.in_path.endswith(".zip"):
            zh = zipfile.ZipFile(self.in_path)
            # Assume a single zipped file
            names = zh.namelist()
            if len(names) != 1:
                raise ValueError("Too many zipped files")
            else:
                fh = zh.open(names[0])

        start = time.time()
        x = 0
        done_x = 0
        l = 1
        while l:
            l = fh.readline()
            if not l:
                break
            # Find id and check if already exists before processing JSON
            what = self.get_identifier_raw(l)
            if what and what in self.out_cache:
                done_x += 1
                if not done_x % 10000:
                    logger.info(f"Skipping past {done_x} {time.time() - start}")
                continue
            # Cache assumes JSON as input, so need to parse it
            try:
                js = json.loads(l)
            except:
                raise
            x += 1
            try:
                new = self.post_process_json(js)
            except:
                logger.error(f"Failed to process {l}")
                raise
            if new is not None:
                if not what:
                    what = self.get_identifier_json(new)
                    if not what:
                        logger.warning(l)
                        raise NotImplementedError(
                            f"is get_identifier_raw or _json implemented for {self.__class__.__name__}?"
                        )
                self.out_cache[what] = new
            if not x % 10000:
                t = time.time() - start
                xps = x / t
                ttls = self.total / xps
                logger.info(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls / 3600} hrs)")
        fh.close()
        self.out_cache.commit()

    def load_export2(self):
        where = self.config["all_configs"].dumps_dir +"/harvested"
        fn = os.path.join(where, f"export_{self.config['name']}.jsonl.gz")
        if not os.path.exists(fn):
            logger.error(fn)
            return None

        self.out_cache.defer_commits(every=1000)

        fh = gzip.open(fn, "rt")
        x = 0
        start = time.time()
        for line in fh:
            x += 1
            ident, json_data = line.strip().split("\t", 1)
            js = json.loads(json_data)
            self.out_cache[ident] = js
            self.out_cache.checkpoint()
            if not x % 10000:
                t = time.time() - start
                xps = x / t
                logger.info(f"{x} in {t} = {xps}/s")
        fh.close()
        self.out_cache.flush()


    def load_export(self):
        where = self.config["all_configs"].dumps_dir
        zipfn = os.path.join(where, f"export_{self.config['name']}.zip")
        if not os.path.exists(zipfn):
            zipfn = os.path.join(where, f"{self.config['name']}.zip")
        if os.path.exists(zipfn):
            zh = zipfile.ZipFile(zipfn, "r", compression=zipfile.ZIP_BZIP2)
        else:
            logger.error("Could not find export zip")
            return None
        start = time.time()
        x = 0
        idents = zh.namelist()
        total = len(idents)
        logger.info(total)
        for ident in idents:
            fh = zh.open(ident)
            data = fh.read()
            fh.close()
            try:
                data = data.decode("utf-8")
                js = json.loads(data)
            except Exception as e:
                logger.error(e)
                continue
            x += 1
            self.out_cache[ident] = js["data"]
            if not x % 10000:
                t = time.time() - start
                xps = x / t
                ttls = total / xps
                logger.info(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls / 3600} hrs)")
        zh.close()
