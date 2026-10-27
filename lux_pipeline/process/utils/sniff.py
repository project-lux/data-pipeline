"""Work out the ``input_files`` type of a dump from what is in it.

`Loader.guess_fmt()` and `lux guess-dump-type` both ask `sniff_spec(path)`
for the spec (the path language in ``lux_pipeline/process/base/loader.py``)
that loads a file or directory. Nothing here needs a config or writes
anything.

A file is never read whole. The sniffer reads its first megabyte (inflated
when it is compressed) and opens an archive only far enough to look at its
first few members. The one bounded exception is a single JSON object: to
tell a dict of records from one record it is parsed when it is under
`WHOLE_OBJECT_BELOW`.

The evidence is data, so a new signature is one row: `MAGIC` for leading
bytes, `TEXT_RULES` for text the loader cannot read, `CONTENT_RULES` for
what the first character of the content says.
"""

import bz2
import gzip
import json
import os
import tarfile
import zipfile
import zlib
from collections import Counter
from dataclasses import dataclass, field

from lxml import etree

# how much content (after inflation) the decisions are made on
SAMPLE_BYTES = 1024 * 1024
# a single JSON object under this is parsed to tell a dict of records from a record
WHOLE_OBJECT_BELOW = 64 * 1024 * 1024
# an array of this size or more is worth a warning: array/raw parses it whole
LARGE_ARRAY = 1024 ** 3
# archive members (and directory files) whose specs are compared
MEMBERS_TO_CHECK = 5
# tar headers read looking for members to check; a tar has no index
TAR_HEADERS_TO_READ = 200
# ignored member names reported per archive
IGNORED_TO_LIST = 10

# (bytes, offset, kind, value): the first row that matches wins
MAGIC = (
    (b"\x1f\x8b", 0, "compression", "gz"),
    (b"BZh", 0, "compression", "bz2"),
    (b"\x78\x01", 0, "compression", "z"),
    (b"\x78\x9c", 0, "compression", "z"),
    (b"\x78\xda", 0, "compression", "z"),
    (b"PK\x03\x04", 0, "container", "zip"),
    (b"ustar", 257, "container", "tar"),
    (b"SQLite format 3\x00", 0, "unplaceable", "SQLite database"),
)

# member names worth opening in an archive; anything else is ignored (the
# .csv beside the .json in the ROR zip). A name without an extension is opened.
MEMBER_EXTENSIONS = (".json", ".jsonl", ".ndjson", ".jsonld", ".xml")
COMPRESSION_EXTENSIONS = {".gz": "gz", ".bz2": "bz2", ".tgz": "gz"}
# one complete object on one line is a record or a one-line JSON Lines file;
# these names say which
LINES_EXTENSIONS = (".jsonl", ".ndjson")


class Unplaceable(Exception):
    """The loader has no spec for this content; the message says what it is."""


@dataclass
class Sniffed:
    """What the sniffer made of one path."""

    path: str
    spec: str | None = None
    reason: str = ""                                # why spec is None
    notes: list = field(default_factory=list)       # things the author should check
    ignored: list = field(default_factory=list)     # archive members not opened
    evidence: dict = field(default_factory=dict)    # magic, head, member, keys

    @property
    def placed(self):
        return self.spec is not None


# --- text the loader cannot read -------------------------------------------


def _is_ntriples(line):
    s = line.rstrip()
    return s.endswith(" .") and (s.startswith("<") or s.startswith("_:"))


def _is_delimited(line):
    return ("\t" in line or "," in line) and not any(c in line for c in "{[<")


# (predicate on the first line, what it is): checked before CONTENT_RULES
TEXT_RULES = (
    (_is_ntriples, "N-Triples"),
    (_is_delimited, "delimited text (tab- or comma-separated)"),
)


# --- bytes -------------------------------------------------------------------


def sniff_magic(head):
    """``(kind, value)`` from leading bytes: ``("compression", "gz")``,
    ``("container", "zip")``, ``("unplaceable", "SQLite database")``, or
    ``(None, None)``."""
    for sig, offset, kind, value in MAGIC:
        if head[offset:offset + len(sig)] == sig:
            if value == "bz2" and not head[3:4].isdigit():
                continue
            return kind, value
    return None, None


def sniff_compression(head):
    """``gz``, ``bz2``, ``z`` or ``None`` for leading bytes."""
    kind, value = sniff_magic(head)
    return value if kind == "compression" else None


def _read(opener, limit, comp=None):
    """Up to `limit` bytes of content from the start of `opener()`,
    inflated when `comp` is set. A truncated or corrupt stream gives what
    could be read."""
    with opener() as fh:
        if comp == "gz":
            src = gzip.GzipFile(fileobj=fh)
        elif comp == "bz2":
            src = bz2.BZ2File(fh)
        elif comp == "z":
            out = bytearray()
            d = zlib.decompressobj()
            try:
                while len(out) < limit:
                    chunk = fh.read(64 * 1024)
                    if not chunk:
                        break
                    out += d.decompress(chunk, limit - len(out))
            except zlib.error:
                pass
            return bytes(out)
        else:
            return fh.read(limit)
        out = bytearray()
        try:
            while len(out) < limit:
                chunk = src.read(min(limit - len(out), 1024 * 1024))
                if not chunk:
                    break
                out += chunk
        except (OSError, EOFError, zlib.error):
            pass
        return bytes(out)


def sniff_container(path):
    """``(container, compression)`` for a path: ``("tar", "gz")``,
    ``("zip", None)``, ``("lmdb", None)``, ``("dir", None)``, ``(None, "gz")``
    for a compressed file that is not an archive, ``(None, None)`` otherwise."""
    if os.path.isdir(path):
        if os.path.exists(os.path.join(path, "data.mdb")):
            return "lmdb", None
        files, subdirs = _listing(path)
        if not files and subdirs:
            return "dirs", None
        return ("dir", None) if files else (None, None)
    opener = lambda: open(path, "rb")  # noqa: E731
    head = _read(opener, 512)
    kind, value = sniff_magic(head)
    if kind == "container":
        return value, None
    if kind == "compression":
        inner = _read(opener, 512, value)
        if sniff_magic(inner) == ("container", "tar"):
            return "tar", value
        return None, value
    return None, None


# --- content -----------------------------------------------------------------


def _with_compression(spec, comp):
    """``lines/json`` + ``gz`` -> ``lines.gz/json``; ``json`` -> ``json.gz``."""
    if not comp:
        return spec
    first, sep, rest = spec.partition("/")
    return f"{first}.{comp}{sep}{rest}"


def _hint_keys(record):
    """The keys a loader author wants for extract_identifier."""
    if not isinstance(record, dict):
        return type(record).__name__
    keys = [k for k in ("id", "@id", "uri", "about") if k in record]
    return keys or list(record)[:8]


def _first_value(text, start=0):
    """The first JSON value at or after `start`, or ``None``."""
    i = start
    while i < len(text) and text[i] in " \t\r\n,[":
        i += 1
    try:
        return json.JSONDecoder().raw_decode(text, i)[0]
    except ValueError:
        return None


def _non_blank_lines(text, n):
    out = []
    for line in text.split("\n"):
        if line.strip():
            out.append(line.strip())
            if len(out) == n:
                break
    return out


def _parse_object(line):
    try:
        obj = json.loads(line)
    except ValueError:
        return None
    return obj if isinstance(obj, dict) else None


def _sniff_array(ctx):
    text = ctx["text"].lstrip()
    lines = _non_blank_lines(text, 3)
    first = lines[0].lstrip("[").strip().rstrip(",").removesuffix("]").strip().rstrip(",")
    if not first and len(lines) > 1:
        first, lines = lines[1].lstrip(",").strip().rstrip(",").removesuffix("]").strip(), lines[1:]
    record = _parse_object(first) if first.startswith("{") else None
    if record is not None and len(lines) > 1:
        ctx["keys"] = _hint_keys(record)
        return "arraylines/json"
    item = _first_value(text, 1)
    ctx["keys"] = _hint_keys(item)
    if item is not None and not isinstance(item, dict):
        ctx["notes"].append(f"the array's items are {type(item).__name__}, not objects")
    if ctx["size"] >= LARGE_ARRAY:
        ctx["notes"].append(f"array/raw parses all {ctx['size']:,} bytes at once; "
                            "it is not written one record per line, so arraylines cannot read it")
    return "array/raw"


def _sniff_object(ctx):
    text = ctx["text"]
    lines = _non_blank_lines(text, 2)
    record = _parse_object(lines[0])
    if record is not None:
        ctx["keys"] = _hint_keys(record)
        if len(lines) > 1 and lines[1].startswith("{"):
            return "lines/json"
        if len(lines) == 1 and _base_name(ctx["name"]).endswith(LINES_EXTENSIONS):
            ctx["notes"].append("one record on one line; the name says JSON Lines")
            return "lines/json"
    data = ctx["whole"](WHOLE_OBJECT_BELOW + 1)
    if len(data) > WHOLE_OBJECT_BELOW:
        ctx["notes"].append("one JSON object of 64 MB or more, not one per line: "
                            "taken as a single record; verify")
        ctx["keys"] = _hint_keys(_first_value(text) if record is None else record)
        return "json"
    try:
        obj = json.loads(data.decode("utf-8-sig"))
    except ValueError as e:
        # JSON Lines whose first line is longer than the sample
        whole = _non_blank_lines(data.decode("utf-8-sig", errors="replace"), 2)
        if len(whole) == 2 and all(_parse_object(x) is not None for x in whole):
            ctx["keys"] = _hint_keys(_parse_object(whole[0]))
            return "lines/json"
        raise Unplaceable(f"starts like JSON but does not parse: {e}")
    if obj and all(isinstance(v, dict) for v in obj.values()):
        ctx["keys"] = _hint_keys(next(iter(obj.values())))
        ctx["notes"].append(f"an object of {len(obj):,} record(s), keyed by "
                            f"e.g. {next(iter(obj))!r}")
        return "dict/raw"
    ctx["keys"] = _hint_keys(obj)
    return "json"


def _sniff_xml(ctx):
    lines = _non_blank_lines(ctx["text"], 2)
    if len(lines) > 1 and lines[1].startswith("<") and not lines[0].startswith("<?"):
        try:
            etree.fromstring(lines[0].encode("utf-8"))
            return "lines/other"
        except etree.XMLSyntaxError:
            pass
    parser = etree.XMLPullParser(events=("start", "end"))
    depth = 0
    children = []          # tags of the root's complete children
    root_closed = False
    error = None
    try:
        parser.feed(ctx["sample"])
    except etree.XMLSyntaxError as e:
        error = e
    for event, elem in parser.read_events():
        if event == "start":
            depth += 1
        else:
            depth -= 1
            if depth == 1:
                children.append(etree.QName(elem).localname)
            elif depth == 0:
                root_closed = True
    if error is not None and (root_closed or "Extra content" in str(error)):
        raise Unplaceable("several XML documents in one file; "
                          "neither xmls nor other can read it")
    ctx["keys"] = f"{len(children)} complete child element(s) in the sample"
    if len(children) >= 2 and len(set(children)) == 1:
        return "xmls/other"
    return "other"


# first non-whitespace character of the content -> how to read it
CONTENT_RULES = {
    "[": _sniff_array,
    "{": _sniff_object,
    "<": _sniff_xml,
}


def _base_name(name):
    """The name without a compression extension, lower case."""
    name = name.lower()
    for ext in COMPRESSION_EXTENSIONS:
        if name.endswith(ext):
            return name[: -len(ext)] + (".tar" if ext == ".tgz" else "")
    return name


# --- one stream --------------------------------------------------------------


def _sniff_stream(found, opener, size, nested):
    """Fill in `found` for the content `opener()` gives. `nested` is true
    inside an archive, where the loader cannot open another archive."""
    head = _read(opener, 512)
    found.evidence["magic"] = head[:8].hex(" ")
    if not head:
        raise Unplaceable("empty file")
    kind, value = sniff_magic(head)
    if kind == "unplaceable":
        raise Unplaceable(value)
    if kind == "container":
        if nested:
            raise Unplaceable(f"a {value} archive inside an archive")
        if value == "zip":
            return _sniff_zip(found, opener)
        return _sniff_tar(found, opener, None)
    comp = value if kind == "compression" else None
    if comp and sniff_magic(_read(opener, 512, comp)) == ("container", "tar"):
        if nested:
            raise Unplaceable("a tar archive inside an archive")
        return _sniff_tar(found, opener, comp)

    sample = _read(opener, SAMPLE_BYTES, comp)
    text = sample.decode("utf-8", errors="replace").lstrip("﻿")
    stripped = text.lstrip()
    found.evidence["head"] = stripped[:60].replace("\n", "\\n")
    if not stripped:
        raise Unplaceable("empty file" if not comp else f"empty after {comp} inflation")
    first_line = stripped.split("\n", 1)[0]
    for rule, what in TEXT_RULES:
        if rule(first_line):
            raise Unplaceable(what)
    rule = CONTENT_RULES.get(stripped[0])
    if rule is None:
        raise Unplaceable(f"unrecognised content starting {stripped[:20]!r}")
    ctx = {"text": text, "sample": sample, "size": size, "name": found.path,
           "notes": found.notes, "keys": None,
           "whole": lambda limit: _read(opener, limit, comp)}
    spec = rule(ctx)
    if ctx["keys"] is not None:
        found.evidence["keys"] = ctx["keys"]
    found.spec = _with_compression(spec, comp)
    return found


def _sniff_one(name, opener, size=-1, nested=False):
    found = Sniffed(name)
    try:
        _sniff_stream(found, opener, size, nested)
    except Unplaceable as e:
        found.spec, found.reason = None, str(e)
    except (OSError, EOFError, zlib.error, tarfile.TarError, zipfile.BadZipFile) as e:
        found.spec, found.reason = None, f"cannot read: {e}"
    return found


# --- several members ---------------------------------------------------------


def _wanted_member(name):
    """Whether an archive member is opened; ``False`` lists it as ignored."""
    base = name.rstrip("/").rsplit("/", 1)[-1]
    if base.startswith(".") or name.startswith("__MACOSX/"):
        return False
    base = _base_name(base)
    if "." not in base:
        return True
    return base.endswith(MEMBER_EXTENSIONS)


def _combine(found, container, members):
    """`found.spec` is ``<container>/<the members' spec>``, by majority."""
    placed = [m for m in members if m.placed]
    if not members:
        raise Unplaceable("every member is ignored" if found.ignored else "no members")
    if not placed:
        raise Unplaceable(f"no member can be placed ({members[0].path}: {members[0].reason})")
    counts = Counter(m.spec for m in placed)
    spec, _ = counts.most_common(1)[0]
    if len(counts) > 1 or len(placed) < len(members):
        seen = ", ".join(f"{m.path}: {m.spec or m.reason}" for m in members)
        found.notes.append(f"members disagree, took the majority ({seen})")
    first = next(m for m in placed if m.spec == spec)
    found.evidence["member"] = first.path
    for k in ("head", "keys"):
        if k in first.evidence:
            found.evidence[k] = first.evidence[k]
    found.notes.extend(first.notes)
    found.spec = f"{container}/{spec}"
    return found


def _note_ignored(found, ignored):
    found.ignored = ignored[:IGNORED_TO_LIST]
    if len(ignored) > IGNORED_TO_LIST:
        found.ignored.append(f"... and {len(ignored) - IGNORED_TO_LIST} more")
    if ignored:
        found.notes.append("ignored members are still read by the base Loader: "
                           "the loader needs a should_process_item override")


def _sniff_zip(found, opener):
    with opener() as fh, zipfile.ZipFile(fh) as zf:
        infos = [i for i in zf.infolist() if not i.is_dir()]
        wanted = [i for i in infos if _wanted_member(i.filename)]
        _note_ignored(found, [i.filename for i in infos if not _wanted_member(i.filename)])
        members = [_sniff_one(i.filename, lambda i=i: zf.open(i), i.file_size, nested=True)
                   for i in wanted[:MEMBERS_TO_CHECK]]
    return _combine(found, "zip", members)


def _sniff_tar(found, opener, comp):
    with opener() as fh, tarfile.open(fileobj=fh, mode=f"r:{comp}" if comp else "r:") as tf:
        members, ignored = [], []
        for _ in range(TAR_HEADERS_TO_READ):
            ti = tf.next()
            if ti is None or len(members) == MEMBERS_TO_CHECK:
                break
            if not ti.isfile():
                continue
            if not _wanted_member(ti.name):
                ignored.append(ti.name)
                continue
            members.append(_sniff_one(ti.name, lambda ti=ti: tf.extractfile(ti), ti.size,
                                      nested=True))
        _note_ignored(found, ignored)
    container = f"tar.{comp}" if comp else "tar"
    return _combine(found, container, members)


# --- directories -------------------------------------------------------------


def _listing(path):
    """``(files, subdirs)``: sorted full paths, dot-names left out."""
    files, subdirs = [], []
    for name in sorted(os.listdir(path)):
        if name.startswith("."):
            continue
        full = os.path.join(path, name)
        if os.path.isdir(full):
            subdirs.append(full)
        elif os.path.isfile(full):
            files.append(full)
    return files, subdirs


def _leaves(path, n):
    """The first `n` files under `path`, in the order dirs/ walks them."""
    out = []
    for root, dirs, files in os.walk(path):
        dirs[:] = sorted(d for d in dirs if not d.startswith("."))
        out.extend(os.path.join(root, f) for f in sorted(files) if not f.startswith("."))
        if len(out) >= n:
            break
    return out[:n]


def _sniff_lmdb(found, path):
    """An LMDB distribution store: ``lmdb/json``, with the value's
    compression on the format step."""
    import lmdb

    spec = "lmdb/json"
    try:
        env = lmdb.open(path, max_dbs=3, readonly=True, lock=False)
        try:
            with env.begin() as txn:
                db = env.open_db(b"data", txn=txn)
                cursor = txn.cursor(db=db)
                if cursor.first():
                    key, value = cursor.key(), cursor.value()
                    found.evidence["member"] = key.decode("utf-8", errors="replace")
                    found.evidence["magic"] = value[:8].hex(" ")
                    comp = sniff_compression(value)
                    if comp:
                        spec = f"lmdb/json.{comp}"
                    found.notes.append("keys are <source>:<id>; a loader reads only "
                                       "its own source's prefix")
                else:
                    found.notes.append("the store's data database is empty")
        finally:
            env.close()
    except Exception as e:
        found.notes.append(f"could not read the store to check its values: {e}")
    found.spec = spec
    return found


def _sniff_directory(found, path):
    container, _ = sniff_container(path)
    if container == "lmdb":
        return _sniff_lmdb(found, path)
    if container is None:
        raise Unplaceable("empty directory")
    if container == "dirs":
        leaves = _leaves(path, MEMBERS_TO_CHECK)
    else:
        files, subdirs = _listing(path)
        leaves = files[:MEMBERS_TO_CHECK]
        if subdirs:
            found.notes.append(f"dir reads only the files directly in {path}; "
                               f"{len(subdirs)} sub-directory(ies) are not read")
    members = [_sniff_one(os.path.relpath(f, path), lambda f=f: open(f, "rb"),
                          os.path.getsize(f)) for f in leaves]
    return _combine(found, container, members)


# --- the entry points --------------------------------------------------------


def sniff_spec(path):
    """A `Sniffed` for a file or a directory of records; ``spec`` is
    ``None`` and ``reason`` says why when the loader cannot read it."""
    found = Sniffed(path)
    try:
        if os.path.isdir(path):
            return _sniff_directory(found, path)
        if not os.path.exists(path):
            raise Unplaceable("no such file or directory")
        result = _sniff_one(path, lambda: open(path, "rb"), os.path.getsize(path))
        return result
    except Unplaceable as e:
        found.spec, found.reason = None, str(e)
    except (OSError, EOFError, zlib.error, tarfile.TarError, zipfile.BadZipFile) as e:
        found.spec, found.reason = None, f"cannot read: {e}"
    return found


def _is_one_input(path):
    """Whether a directory is one ``input_files`` entry as a whole: an
    LMDB store, or nothing but sub-directories (none of them a store)."""
    container, _ = sniff_container(path)
    if container == "lmdb":
        return True
    if container == "dirs":
        _, subdirs = _listing(path)
        return not any(os.path.exists(os.path.join(d, "data.mdb")) for d in subdirs)
    return False


def discover_input_files(path, relative_to=None):
    """A `Sniffed` per entry of a dumps directory, sorted by name, with
    ``path`` relative to `relative_to` (default: the directory itself) so
    the block is portable; a path outside it stays absolute.

    Every regular file is an entry, and so is every sub-directory (as
    ``dir``, ``dirs`` or ``lmdb``); sub-directories are not listed file by
    file. A directory that is an LMDB store, or holds only sub-directories,
    is one entry (``.``). A file given as `path` is its own entry.
    """
    path = os.path.abspath(path)
    if relative_to is None:
        relative_to = path if os.path.isdir(path) else os.path.dirname(path)
    relative_to = os.path.abspath(relative_to)

    if os.path.isdir(path) and not _is_one_input(path):
        files, subdirs = _listing(path)
        targets = sorted(files + subdirs)
    else:
        targets = [path]

    out = []
    for t in targets:
        found = sniff_spec(t)
        rel = os.path.relpath(t, relative_to)
        found.path = t if rel == ".." or rel.startswith(".." + os.sep) else rel
        out.append(found)
    return out
