"""Load dump files into a datacache.

A source config describes its dump files as ``input_files``: a dict of load
type (``records`` by default) to a list of ``{"path"|"url", "type"}``
entries, where ``type`` is a small path language::

    (container[.compression]/)*format[.compression]

    containers:  dir dirs pair zip tar lines dict array arraylines xmls lmdb
    formats:     json raw other tabjson
    compression: gz bz2 z

    dir         a directory of files
    dirs, pair  a directory hierarchy of files (pair: two-character subdirs)
    zip, tar    an archive of members
    lines       a file where each line is a member
    dict        a JSON file whose top level is an object; each value is a member
    array       a JSON file whose top level is an array; each item is a member
    arraylines  a JSON array written one record per line
    xmls        an XML file; each child of the root is a member
    lmdb        an LMDB distribution store (lux export --format lmdb); each value is a member
    json        a JSON record (a file, a member, or an already parsed item)
    tabjson     ``<identifier><TAB><JSON record>``, as a line of
                ``lux export --type external``; a record of only ``{"data": ...}``
                is unwrapped
    raw         an already parsed JSON item (from dict / array)
    other       anything else, kept as text under ``{"data": ...}``

For example ``tar.gz/lines.bz2/json`` is a gzipped tar whose members are
bzip2'd JSON Lines files; a datacache export is ``zip.bz2/json``; an LMDB
store of zlib-compressed records is ``lmdb/json.z``; an external export
(``lux export --type external``) is ``lines.gz/tabjson``.

Loading walks the spec: each container step yields children (paths,
archive members, lines, JSON items) to the next step; the format step
builds ``{"identifier", "data"}`` and stores it. Slicing happens at the
format step (``seen % max_slice == my_slice``), so N workers can read the
same file and every worker must see the children in the same order --
which is why directory listings are sorted.

An entry with ``"slice": "files"`` is sliced by file instead: each worker
takes every ``max_slice``-th file of the spec's first directory step
(``dir``, ``dirs``, ``pair``) and every record in it, so no worker reads
another's files -- for a dump of many large compressed files, where reading
every file in every worker costs N times the I/O. An entry with no
directory step is itself the unit: every ``max_slice``-th such entry.
Files are counted in order across the entries, and a file
``should_process_item`` refuses is not counted.

An entry may say where each record's identifier comes from, with
``"identifier"`` (for the ``json`` and ``other`` formats; ``raw`` items are
always identified from the record, ``tabjson`` lines carry theirs):

    (no key)    today's rule: the file or member name, its last component
                cut at the first "."; a line or item with no name is
                identified from the record (``extract_identifier``)
    "name"      the same
    "record"    {"from": "record", "fields": ["id"], "form": ["last", "extension"]}
    {"from": "name" | "record", "fields": [...], "form": [...]}

``fields`` (record only, default ``["id"]``) are the record's top-level keys
tried in order; ``form`` (default ``["last", "extension"]``) are steps
applied in order to the name or value:

    namespace   strip the source's ``namespace`` (record values only); a
                value outside it is logged and the record skipped
    last        keep the last path segment
    extension   strip one trailing .gz / .bz2, then one of .json .jsonld
                .xml .rdf .ttl .nt .txt (so "1.2.3" keeps its dots)

so ``{"from": "record", "form": ["namespace", "extension"]}`` makes
``http://example.org/data/class/1.2.3.json`` (namespace
``http://example.org/data/``) the identifier ``class/1.2.3``. A name is an
archive member's name inside its archive, or a file's own name. A source
class that overrides ``extract_identifier`` decides record identifiers
itself; the rule then only chooses record over name.

``prepare(path=..., path_type=...)`` (``lux load --file`` / ``--file-type``)
replaces the configured file for one run: see `Loader.override_file`.

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
from lux_pipeline.process.utils.sniff import sniff_spec

logger = logging.getLogger("lux_pipeline")

CONTAINERS = ("dir", "dirs", "pair", "zip", "tar", "lines", "dict", "array",
              "arraylines", "graph", "xmls", "lmdb")
FORMATS = ("json", "raw", "other", "tabjson")
COMPRESSIONS = ("gz", "bz2", "z")
DIRECTORIES = ("dir", "dirs", "pair")
SLICES = ("records", "files")
# the input_files "identifier" rule
IDENTIFIER_SOURCES = ("name", "record")
IDENTIFIER_STEPS = ("namespace", "last", "extension")
DEFAULT_IDENTIFIER_FORM = ["last", "extension"]
COMPRESSION_SUFFIXES = (".gz", ".bz2")
RECORD_EXTENSIONS = (".json", ".jsonld", ".xml", ".rdf", ".ttl", ".nt", ".txt")

# files above this are not read through to count their lines for progress
COUNT_LINES_BELOW = 100_000_000


# files an operating system leaves in a directory or archive, never records:
# macOS's __MACOSX/ tree and ._<name> AppleDouble files, Finder's .DS_Store,
# Windows' Thumbs.db and desktop.ini
OS_METADATA_DIRS = ("__MACOSX",)
OS_METADATA_FILES = (".DS_Store", "Thumbs.db", "desktop.ini")
UTF8_BOM = b"\xef\xbb\xbf"


def is_os_metadata(name):
    """Whether a file or member name is an operating system's metadata."""
    parts = [p for p in str(name).replace("\\", "/").split("/") if p]
    if not parts:
        return False
    return (any(p in OS_METADATA_DIRS for p in parts[:-1])
            or parts[-1] in OS_METADATA_FILES or parts[-1].startswith("._"))


# extract_identifier's answer for a value outside the source's namespace
_OUTSIDE = object()


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
        self.files_seen = 0        # file units met under "slice": "files", all slices
        self._file_level = None    # len(steps) at the step whose children are sliced
        self._slicing_files = False
        self._lmdb_envs = {}
        self._ppj_takes_identifier = None
        self._identifier_rule = None   # the current entry's "identifier", parsed
        self.outside_namespace = 0     # records skipped: identifier outside the namespace
        self.step_functions = {
            "dir": self.iterate_directory,
            "dirs": self.iterate_directories,
            "pair": self.iterate_directories,
            "zip": self.iterate_zip,
            "tar": self.iterate_tar,
            "lines": self.iterate_lines,
            "arraylines": self.iterate_arraylines,
            "graph": self.iterate_graph,
            "dict": self.iterate_dict,
            "array": self.iterate_array,
            "xmls": self.iterate_xmls,
            "lmdb": self.iterate_lmdb,
            "json": self.make_json,
            "tabjson": self.make_tabjson,
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
        """A spec for an entry without ``type``, from the content of `path`
        (lux_pipeline/process/utils/sniff.py, as `lux guess-dump-type`
        prints it). The guess is logged so a build log shows every
        assumption. A path that does not exist yet is guessed from its
        name: only the common shapes."""
        if os.path.exists(path):
            found = sniff_spec(path)
            if not found.placed:
                raise ValueError(f"Cannot guess the type of {path} ({found.reason}); "
                                 "set 'type' in input_files")
            logger.info(f"{self.name}: guessed {found.spec} for {path}")
            return self.process_fmt(found.spec)
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

    def file_level(self, spec):
        """For a spec sliced by file, ``len(steps)`` at its first directory
        step (the step whose children are the files), or ``None`` when it
        has none."""
        for i, step in enumerate(spec):
            if step[0] in DIRECTORIES:
                return len(spec) - i
        return None

    def _take_file(self):
        """Count one file unit; whether it is this slice's."""
        idx = self.files_seen
        self.files_seen += 1
        return self.max_slice <= 1 or idx % self.max_slice == self.my_slice

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
        files = [f for f in sorted(os.listdir(path)) if not is_os_metadata(f)]
        if len(remaining) == 1:
            self._add_total(len(files))
        for f in files:
            full = os.path.join(path, f)
            if os.path.isfile(full):
                yield full

    def iterate_directories(self, path, comp, remaining):
        for f in sorted(os.listdir(path)):
            if f in OS_METADATA_DIRS or is_os_metadata(f):
                continue
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
            names = [n for n in zh.namelist() if not n.endswith("/") and not is_os_metadata(n)]
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
                if ti.isfile() and not is_os_metadata(ti.name):
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

    def iterate_graph(self, path, comp, remaining):
        """A JSON-LD document: the items of its top-level ``@graph`` array."""
        with self.file_opener(path, comp) as fh:
            data = json.load(fh)
        items = data.get("@graph", []) if isinstance(data, dict) else []
        if len(remaining) == 1:
            self._add_total(len(items))
        for v in items:
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
        if ident is _OUTSIDE:
            return self._skip_outside(data, parent, None)
        if not ident:
            raise ValueError(f"Could not get an identifier in {self.name} while in {parent}")
        return {"identifier": ident, "data": data}

    def make_json(self, path, comp, parent):
        ident = self.name_identifier(path)
        if isinstance(path, dict):
            data = path
        else:
            with self.file_opener(path, comp) as fh:
                raw = fh.read()
            if raw.startswith(UTF8_BOM):
                raw = raw[len(UTF8_BOM):]
            try:
                data = json.loads(raw)
            except ValueError as e:
                what = f"{self.member_name(path, parent)} ({len(raw)} bytes)"
                raise ValueError(f"{self.name}: {what} is not JSON: {e}; it starts "
                                 f"{bytes(raw[:60])!r}") from e
        try:
            data = self._post_process_json(data, ident)
        except Exception:
            logger.debug(f"Failed to post_process {ident} in {self.name}: {data!r:.200}")
            raise
        if data is None:
            return None
        if not ident:
            ident = self.extract_identifier(data)
            if ident is _OUTSIDE:
                return self._skip_outside(data, parent, path)
            if not ident:
                raise ValueError(f"Could not get an identifier in {self.name} while in {parent}/{path}")
        return {"identifier": ident, "data": data}

    def member_name(self, path, parent):
        """A record's file or archive member, for messages; a line or
        item has none, so its container step is named instead."""
        if isinstance(path, Pointer):
            return path.get_name()
        if isinstance(path, (str, os.PathLike)):
            return os.fspath(path)
        name = getattr(path, "name", None)
        if isinstance(name, str):
            return name
        return f"a record (in {parent[0] if parent else 'the file'})"

    def make_tabjson(self, path, comp, parent):
        """``<identifier><TAB><JSON>``: the identifier is the text before
        the first tab, the record the JSON after it. ``{"data": record}``
        (what the external export writes) is unwrapped."""
        with self.file_opener(path, comp) as fh:
            line = fh.read()
        if isinstance(line, bytes):
            line = line.decode("utf-8")
        ident, sep, text = line.strip().partition("\t")
        if not sep or not ident:
            raise ValueError(f"No <identifier><TAB> prefix in {self.name} while in {parent}: "
                             f"{line[:80]!r}")
        data = json.loads(text)
        if isinstance(data, dict) and list(data) == ["data"]:
            data = data["data"]
        data = self._post_process_json(data, ident)
        if data is None:
            return None
        return {"identifier": ident, "data": data}

    def make_other(self, path, comp, parent):
        ident = self.name_identifier(path)
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
            if ident is _OUTSIDE:
                return self._skip_outside(data, parent, path)
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
        """The identifier from the record itself. With an entry's
        ``identifier`` rule taking it from the record: its ``fields`` in
        order, shaped by its ``form`` (`_OUTSIDE` when ``namespace`` does
        not match). Otherwise ``id``, as `make_identifier` shapes a name."""
        rule = self._identifier_rule
        if rule is not None and rule["from"] == "record":
            if not isinstance(data, dict):
                return None
            for field in rule["fields"]:
                value = data.get(field)
                if value not in (None, ""):
                    return self.shape_identifier(str(value), rule["form"])
            return None
        if isinstance(data, dict) and "id" in data:
            return self.make_identifier(data["id"])
        return None

    def name_identifier(self, path):
        """The identifier a file or member name gives, under the current
        entry's rule: None when the rule takes it from the record, the
        name shaped by its ``form`` when it takes it from the name, and
        `make_identifier` (today's rule) without one."""
        rule = self._identifier_rule
        if rule is None:
            return self.make_identifier(path)
        if rule["from"] == "record":
            return None
        if isinstance(path, Pointer):
            name = path.get_name()
        elif isinstance(path, (str, os.PathLike)):
            name = os.path.basename(os.fspath(path).rstrip("/"))
        else:
            return None
        ident = self.shape_identifier(name, rule["form"])
        return None if ident is _OUTSIDE else ident

    def shape_identifier(self, value, form):
        """`value` through the ``form`` steps; `_OUTSIDE` when it is not
        under the source's namespace, None when nothing is left."""
        for step in form:
            if step == "namespace":
                ns = self.config.get("namespace") or ""
                if not value.startswith(ns):
                    return _OUTSIDE
                value = value[len(ns):]
            elif step == "last":
                value = value.rstrip("/").rsplit("/", 1)[-1]
            elif step == "extension":
                for suffix in COMPRESSION_SUFFIXES:
                    if value.endswith(suffix):
                        value = value[: -len(suffix)]
                        break
                for ext in RECORD_EXTENSIONS:
                    if value.endswith(ext):
                        value = value[: -len(ext)]
                        break
        return value or None

    def _skip_outside(self, data, parent, path):
        self.outside_namespace += 1
        where = f"{parent}/{path}" if path is not None else f"{parent}"
        value = next((data.get(f) for f in self._identifier_rule["fields"]
                      if isinstance(data, dict) and data.get(f)), None)
        logger.warning(f"{self.name}: skipped a record in {where}: its identifier {value!r} "
                       f"is not under the namespace {self.config.get('namespace')!r}")
        return None

    def entry_identifier(self, entry, spec):
        """An ``input_files`` entry's ``identifier`` rule, checked and
        expanded (see the module docstring); None for today's rule.
        ValueError for one that cannot apply."""
        rule = entry.get("identifier")
        if rule is None or rule == "name":
            return None
        if rule == "record":
            rule = {"from": "record"}
        if not isinstance(rule, dict):
            raise ValueError(f"{self.name}'s input_files identifier should be \"name\", "
                             f"\"record\" or an object, not {rule!r}")
        unknown = set(rule) - {"from", "fields", "form"}
        if unknown:
            raise ValueError(f"{self.name}'s input_files identifier has unknown keys: "
                             f"{', '.join(sorted(unknown))}")
        source = rule.get("from", "record")
        if source not in IDENTIFIER_SOURCES:
            raise ValueError(f"{self.name}'s input_files identifier 'from' is one of "
                             f"{', '.join(IDENTIFIER_SOURCES)}, not {source!r}")
        fields = rule.get("fields", ["id"])
        if isinstance(fields, str):
            fields = [fields]
        if source == "name" and "fields" in rule:
            raise ValueError(f"{self.name}'s input_files identifier: 'fields' is for "
                             f"\"from\": \"record\"")
        if not fields or not all(isinstance(f, str) and f for f in fields):
            raise ValueError(f"{self.name}'s input_files identifier 'fields' should be "
                             f"field names, not {fields!r}")
        form = rule.get("form", DEFAULT_IDENTIFIER_FORM)
        if isinstance(form, str):
            form = [form]
        bad = [f for f in form if f not in IDENTIFIER_STEPS] if isinstance(form, list) else [form]
        if bad:
            raise ValueError(f"{self.name}'s input_files identifier 'form' steps are "
                             f"{', '.join(IDENTIFIER_STEPS)}, not {bad!r}")
        if "namespace" in form:
            if source == "name":
                raise ValueError(f"{self.name}'s input_files identifier: 'namespace' "
                                 f"applies to record values, not names")
            if not self.config.get("namespace"):
                raise ValueError(f"{self.name}'s input_files identifier strips the namespace, "
                                 f"but the source has none")
        fmt = spec[-1][0]
        if fmt == "tabjson":
            raise ValueError(f"{self.name}'s input_files identifier does not apply to "
                             f"tabjson: each line carries its identifier")
        if fmt == "raw" and source == "name":
            raise ValueError(f"{self.name}'s input_files identifier: raw items have no "
                             f"name; use \"from\": \"record\"")
        return {"from": source, "fields": list(fields), "form": list(form)}

    def configured_identifier(self, load_type, spec):
        """The ``identifier`` rule of the config's one entry for
        `load_type`, applied to `spec` (None when there is not exactly
        one, or it has none)."""
        entries = (self.config.get("input_files") or {}).get(load_type) or []
        return self.entry_identifier(entries[0], spec) if len(entries) == 1 else None

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
        if self._slicing_files:
            return True
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
            slice_here = self._file_level is not None and len(steps) == self._file_level
            for child in handler(path, comp, steps[1:]):
                if self.should_process_item(child):
                    if slice_here and not self._take_file():
                        continue
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

    def configured_type(self, load_type):
        """The spec the config gives its file for `load_type`, when it
        configures exactly one: an ``input_files`` entry's ``type``, or
        ``dumpFileType`` for records. ``None`` otherwise."""
        entries = (self.config.get("input_files") or {}).get(load_type) or []
        if len(entries) == 1:
            return entries[0].get("type")
        if not entries and load_type == "records":
            return self.config.get("dumpFileType")
        return None

    def configured_slice(self, load_type):
        """How the config's one entry for `load_type` is sliced
        (``records`` when there is not exactly one)."""
        entries = (self.config.get("input_files") or {}).get(load_type) or []
        return self.entry_slice(entries[0]) if len(entries) == 1 else "records"

    def entry_slice(self, entry):
        """An ``input_files`` entry's ``slice``: ``records`` (default) or
        ``files``; ValueError for anything else."""
        how = entry.get("slice") or "records"
        if how not in SLICES:
            raise ValueError(f"Unknown slice {how!r} in {self.name}'s input_files "
                             f"(one of {', '.join(SLICES)})")
        return how

    def sniffed_type(self, path):
        """The spec `path`'s content says it has (`sniff_spec`, as `lux
        guess-dump-type` prints it), or ``None`` when it cannot be placed."""
        try:
            found = sniff_spec(path)
        except Exception as e:
            logger.debug(f"{self.name}: could not sniff {path}: {e}")
            return None
        return found.spec if found.placed else None

    def override_file(self, path, path_type, load_type):
        """Read `path` instead of the configured file. A loader whose
        config describes its files (``uses_dsl``) reads it with
        `path_type`, else the type its content shows, else the configured
        file's type: the replacement need not be the shape the config
        describes (an ``identifier<TAB>json`` export for a source whose
        configured export is a zip, say). For ``export`` with none of
        those (main's export format) and for a legacy ``load()`` loader
        the path replaces ``export_path`` / ``in_path``, which those read.
        A legacy loader decides for itself how to read a file, so
        `path_type` is refused for one."""
        sniffed = None if path_type else self.sniffed_type(path)
        if (load_type == "export" and not path_type and not sniffed
                and not self.configured_type("export")):
            self.export_path = path
            self.my_files = []
            logger.info(f"{self.name}: loading the export from {path}")
            return
        if not self.uses_dsl and load_type == "records":
            # a legacy loader reads its records its own way, from in_path;
            # only a base Loader can be put on the DSL with a type
            if path_type and type(self).load is not Loader.load:
                raise ValueError(f"{self.name}'s loader reads its file its own way; "
                                 f"--file-type does not apply to it")
            if not path_type:
                self.in_path = path
                logger.info(f"{self.name}: loading {path} instead of "
                            f"{self.config.get('dumpFilePath')}")
                return
        if path_type:
            fmt, why = path_type, "given"
        elif sniffed:
            fmt, why = sniffed, "from its content"
        else:
            fmt, why = self.configured_type(load_type), "configured"
        if not fmt:
            raise ValueError(f"Cannot tell the type of {path}; give --file-type")
        self.uses_dsl = True
        spec = self.process_fmt(fmt)
        # the configured slicing carries over to a directory read the same
        # way; a single file given in its place is sliced by record
        info = {"path": path, "fmt": spec}
        if self.configured_slice(load_type) == "files" and self.file_level(spec) is not None:
            info["slice"] = "files"
        # where identifiers come from is the source's, whatever the file
        rule = self.configured_identifier(load_type, spec)
        if rule is not None:
            info["identifier"] = rule
        self.my_files = [info]
        logger.info(f"{self.name}: loading {path} as {fmt} ({why}) instead of the configured file")

    def prepare(self, my_slice=-1, max_slice=-1, load_type="records", max_records=-1,
                progress=null_progress, path=None, path_type=None):
        """Work out which files this loader reads, as ``my_files``:
        ``[{"path", "fmt"}]`` with the spec parsed. ``uses_dsl`` says
        whether the config describes its files at all; when it does not,
        the manager calls the legacy ``load()``. `path` (and `path_type`)
        replace the configured file: see `override_file`."""
        self._ensure_dsl()
        Sliced.prepare(self, my_slice, max_slice, progress)
        self.load_type = load_type
        if max_records != -1:
            self.max_records = max_records
        if load_type == "export":
            self.total = -1

        self.files_seen = 0
        files = []
        ifs = self.config.get("input_files") or {}
        self.uses_dsl = bool(ifs) or bool(self.config.get("dumpFileType"))
        self.export_path = None
        if path is not None:
            self.override_file(path, path_type, load_type)
            if load_type == "lmdb" and self.my_files:
                self.total = self.lmdb_count_entries(self.my_files[0]["path"])
            self.increment_total = self.total < 0
            return self
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
                info = {"path": path, "fmt": fmtspec}
                if self.entry_slice(p) == "files":
                    info["slice"] = "files"
                rule = self.entry_identifier(p, fmtspec)
                if rule is not None:
                    info["identifier"] = rule
                files.append(info)
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
                self._slicing_files = info.get("slice") == "files"
                self._file_level = self.file_level(info["fmt"]) if self._slicing_files else None
                self._identifier_rule = info.get("identifier")
                if self._slicing_files and self._file_level is None and not self._take_file():
                    continue    # a whole entry is the unit, and another slice's
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
            self._slicing_files = False
            self._file_level = None
            self._identifier_rule = None
            if hasattr(cache, "resume_commits"):
                cache.resume_commits()
            elif hasattr(cache, "commit"):
                cache.commit()
            self.close_temp_files()
            for env in self._lmdb_envs.values():
                env.close()
            self._lmdb_envs = {}
        if self.outside_namespace:
            logger.warning(f"{self.name}: {self.outside_namespace} record(s) skipped: their "
                           f"identifier is not under the namespace {self.config.get('namespace')!r}")
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
        fn = getattr(self, "export_path", None) or os.path.join(where, f"export_{self.config['name']}.jsonl.gz")
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
