"""Export the build's records: MarkLogic JSONL, merged JSONL, N-Triples, sources.

    lux export --max-workers 24                      # run-export.py 0..23 24: export_full_<n>.jsonl
    lux export --max-workers 24 --gz                 # gzipped
    lux export --type merged --max-workers 24        # run-export.py --no-ml: export_merged_<n>.jsonl
    lux export --type nt --max-workers 24            # manage-data.py --nt: lux_<n>.nt.gz in nt_dir
    lux export --type sources --max-workers 24       # run-source-build.py: aat, ulan, lcsh, tgn
    lux export --type external --source aat,wikidata # export_<name>.jsonl.gz for `lux load --type export`
    lux export --cache datacache --source wikidata --max-workers 24   # the whole datacache, 24 slices
    lux export --cache datacache,recordcache --source ycba            # export_ycba_<cache>.jsonl.gz
    lux export --type merged --format lmdb --max-workers 24           # merged.lmdb in distribution_dir
    lux export --type external --source all --format lmdb             # external.lmdb, <source>:<id> keys
    lux export --cache recordcache --source wikidata --format lmdb    # wikidata_recordcache.lmdb
    lux export --my-worker 3 --max-workers 24 --no-ui   # one slice, shell-orchestrated

Replaces `run-export.py`, `run-source-build.py`, `manage-data.py --nt`,
export-source.py, exportExternal.py and the `export_parallel.sh` /
`nt_parallel.sh` loops.

``--cache`` exports every record of the named caches of each ``--source``
(internal, external or results; ``all`` is every internal and external
one), each source's cache split across ``--max-workers``:
``export_<source>_<cache>_<n>.jsonl.gz``, one ``<identifier><TAB>{"data":
...}`` line per record, as ``--type external`` writes. Read them back with
``lux load --source S --file <dir> --file-type dir/lines.gz/tabjson``.
Leave ``--type`` unset with it. JSONL goes to the
``exports_dir``, N-Triples to the ``nt_dir`` (falling back to
``exports_dir``); ``--out-dir`` sends everything to one directory. See
`lux_pipeline.process.tasks.export_task` for how the work is shared out.

``--format lmdb`` builds an LMDB distribution store instead of files, from
``--type merged``, ``--type external`` or ``--cache``, in
``distribution_dir`` (falling back to ``exports_dir``; ``--out-dir``
overrides). A store holds every record zlib-compressed under its key, and
an index of the keys by class, in key order, so one class can be walked
without reading the rest; see `lux_pipeline.storage.distribution` for the
layout. Keys are the record's UUID as 16 bytes for ``merged`` and
``<source>:<identifier>`` text otherwise (what ``lux load --type lmdb``
reads); ``--lmdb-keys`` chooses. The workers write sorted runs and the
store is built from them in key order when they are done. Run as
``--my-worker`` slices, finish with the same command and ``--merge-only``.
"""

from lux_pipeline.process.tasks.export_task import (ALIASES, EXPORT_TYPES, MERGED_TYPES,
                                                    ExportManager, default_sources,
                                                    normalize_type, parse_caches)

from ._handler import CommandHandler as CH
from ._handler import add_profile_arg, enable_profiling


class CommandHandler(CH):
    def __init__(self, cfgs):
        super().__init__(cfgs)
        self.default_source = "merged"

    def add_args(self, ap):
        ap.add_argument("--type", dest="export_type", default=None,
                        choices=[*EXPORT_TYPES, *ALIASES],
                        help="What to export (default marklogic)")
        ap.add_argument("--cache", default=None,
                        help="Export every record of these caches of each --source instead "
                             "(cache types separated by commas, e.g. datacache)")
        ap.add_argument("--format", default="jsonl", choices=("jsonl", "lmdb"),
                        help="jsonl files (default), or an LMDB distribution store for "
                             "--type merged, --type external or --cache")
        ap.add_argument("--lmdb-keys", dest="lmdb_keys", default=None, choices=("uuid", "text"),
                        help="Store keys: 16-byte UUIDs (default for merged) or "
                             "<source>:<identifier> text (default otherwise)")
        ap.add_argument("--merge-only", dest="merge_only", action="store_true",
                        help="Only build the LMDB stores from the runs --my-worker slices wrote")
        ap.add_argument("--gz", action="store_true",
                        help="Gzip the marklogic / merged JSONL")
        ap.add_argument("--chunk", type=int, default=None,
                        help="Records per cache query (default LUX_EXPORT_CHUNK, else 1000)")
        ap.add_argument("--gzip-level", dest="gzip_level", type=int, default=None,
                        help="Compression level (default LUX_EXPORT_GZIP, else 9, for --gz; "
                             "1 for the others)")
        ap.add_argument("--out-dir", dest="out_dir", default=None,
                        help="Write every file here instead of exports_dir / nt_dir")
        add_profile_arg(ap)
        self.extra_args = {"export_type": "export_type", "cache": "cache", "format": "format",
                           "lmdb_keys": "lmdb_keys", "merge_only": "merge_only", "gz": "gz",
                           "chunk": "chunk",
                           "gzip_level": "gzip_level", "out_dir": "out_dir"}

    def resolve_sources(self, args):
        """``--source`` only selects for ``sources`` and ``external``; the
        other types export the merged records whatever it says, so naming
        anything else is an error. ``all`` is the four sources for
        ``sources`` and every external source for ``external``, which
        needs ``--source``."""
        which = args.source
        if parse_caches(getattr(args, "cache", None), getattr(args, "export_type", None)):
            if which is None:
                raise ValueError("--cache needs --source: a name, a list, a group, or all")
            return super().resolve_sources(args)
        t = normalize_type(getattr(args, "export_type", None) or "marklogic")
        if t in MERGED_TYPES:
            if which not in (None, "all", "merged", "results"):
                raise ValueError(f"--type {t} exports the merged records; --source {which} "
                                 f"does not apply")
            return default_sources(t)
        if which is None:
            if t == "external":
                raise ValueError("--type external needs --source: a name, a list, or all")
            return default_sources(t)
        if which in ("all", "external"):
            return default_sources(t) if t == "sources" else list(self.configs.external.keys())
        return super().resolve_sources(args)

    def make_manager(self, wks, args):
        enable_profiling(args)
        return ExportManager(self.configs, wks, args)
