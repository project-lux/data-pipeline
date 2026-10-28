"""Export the build's records: MarkLogic JSONL, merged JSONL, N-Triples, sources.

    lux export --max-workers 24                      # run-export.py 0..23 24: export_full_<n>.jsonl
    lux export --max-workers 24 --gz                 # gzipped
    lux export --type merged --max-workers 24        # run-export.py --no-ml: export_merged_<n>.jsonl
    lux export --type nt --max-workers 24            # manage-data.py --nt: lux_<n>.nt.gz in nt_dir
    lux export --type sources --max-workers 24       # run-source-build.py: aat, ulan, lcsh, tgn
    lux export --type external --source aat,wikidata # export_<name>.jsonl.gz for `lux load --type export`
    lux export --my-worker 3 --max-workers 24 --no-ui   # one slice, shell-orchestrated

Replaces `run-export.py`, `run-source-build.py`, `manage-data.py --nt` and
the `export_parallel.sh` / `nt_parallel.sh` loops. JSONL goes to the
``exports_dir``, N-Triples to the ``nt_dir`` (falling back to
``exports_dir``); ``--out-dir`` sends everything to one directory. See
`lux_pipeline.process.tasks.export_task` for how the work is shared out.
"""

from lux_pipeline.process.tasks.export_task import (ALIASES, EXPORT_TYPES, MERGED_TYPES,
                                                    ExportManager, default_sources,
                                                    normalize_type)

from ._handler import CommandHandler as CH


class CommandHandler(CH):
    def __init__(self, cfgs):
        super().__init__(cfgs)
        self.default_source = "merged"

    def add_args(self, ap):
        ap.add_argument("--type", dest="export_type", default="marklogic",
                        choices=[*EXPORT_TYPES, *ALIASES],
                        help="What to export (default marklogic)")
        ap.add_argument("--gz", action="store_true",
                        help="Gzip the marklogic / merged JSONL")
        ap.add_argument("--chunk", type=int, default=None,
                        help="Records per cache query (default LUX_EXPORT_CHUNK, else 1000)")
        ap.add_argument("--gzip-level", dest="gzip_level", type=int, default=None,
                        help="Compression level (default LUX_EXPORT_GZIP, else 9, for --gz; "
                             "1 for the others)")
        ap.add_argument("--out-dir", dest="out_dir", default=None,
                        help="Write every file here instead of exports_dir / nt_dir")
        self.extra_args = {"export_type": "export_type", "gz": "gz", "chunk": "chunk",
                           "gzip_level": "gzip_level", "out_dir": "out_dir"}

    def resolve_sources(self, args):
        """``--source`` only selects for ``sources`` and ``external``; the
        other types export the merged records whatever it says, so naming
        anything else is an error. ``all`` is the four sources for
        ``sources`` and every external source for ``external``, which
        needs ``--source``."""
        t = normalize_type(getattr(args, "export_type", None) or "marklogic")
        which = args.source
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
        return ExportManager(self.configs, wks, args)
