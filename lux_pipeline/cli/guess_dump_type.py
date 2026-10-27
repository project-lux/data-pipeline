"""Print the input_files block that describes a source's dump files.

    lux guess-dump-type --source ror            # scan ror's dumps directory
    lux guess-dump-type --source all            # every source with a dumps directory
    lux guess-dump-type --path /data/input/dnb  # a directory or file, no config needed
    lux guess-dump-type --source ror --explain  # the evidence behind each guess
    lux guess-dump-type --source ror --type export   # file the entries under "export"

Only the JSON goes to stdout: one source (or --path) prints the
``"input_files": {...}`` fragment a config takes, several print an object
keyed by source. Notes, ignored archive members and the files that cannot
be placed go to stderr; the exit status is 1 when any file could not be
placed. Nothing is written: the guess is pinned in the config by its
author, never made at build time.
"""

import json
import os
import sys

from lux_pipeline.cli._handler import BaseHandler
from lux_pipeline.process.utils.sniff import discover_input_files

GROUPS = ("internal", "external", "results")


def _err(msg):
    print(msg, file=sys.stderr)


class CommandHandler(BaseHandler):
    # --path needs no installation; --source loads the configs in process()
    needs_configs = False

    def add_args(self, ap):
        ap.add_argument("--path", help="A dump file or directory to describe; no config needed")
        ap.add_argument("--type", default="records",
                        help="The input_files load type to file the entries under (default records)")
        ap.add_argument("--explain", action="store_true",
                        help="Print the evidence behind each guess on stderr")

    def source_config(self, name):
        for group in GROUPS:
            cfg = getattr(self.configs, group).get(name)
            if cfg is not None:
                return cfg
        return None

    def source_directory(self, cfg):
        """``(directory or file to scan, directory the loader resolves
        relative paths against)``, or ``None``. The loader's directory is
        ``dumps_dir`` plus the source's own ``dumps_dir``; failing a
        directory there, the one ``dumpFilePath`` is in. When that is the
        shared dumps directory, only ``dumpFilePath`` itself is scanned."""
        base = getattr(self.configs, "dumps_dir", "") or ""
        loader_dir = os.path.join(base, cfg["dumps_dir"]) if cfg.get("dumps_dir") else base
        if cfg.get("dumps_dir") and os.path.isdir(loader_dir):
            return loader_dir, loader_dir
        dfp = cfg.get("dumpFilePath")
        if dfp:
            if not os.path.isabs(dfp):
                dfp = os.path.join(base, dfp)
            parent = os.path.dirname(os.path.normpath(dfp))
            if os.path.normpath(parent) == os.path.normpath(base):
                return (dfp, loader_dir) if os.path.exists(dfp) else None
            if os.path.isdir(parent):
                return parent, loader_dir
        return None

    def describe(self, label, target, relative_to, explain):
        """The entries for `target`, reporting on stderr; ``(entries, failed)``."""
        entries, failed = [], 0
        for found in discover_input_files(target, relative_to):
            where = f"{label}: {found.path}" if label else found.path
            if found.placed:
                entries.append({"path": found.path, "type": found.spec})
            else:
                failed += 1
                _err(f"{where}: cannot place: {found.reason}")
            for name in found.ignored:
                _err(f"{where}: ignored member {name}")
            for note in found.notes:
                _err(f"{where}: note: {note}")
            if explain:
                ev = found.evidence
                bits = [f"{k}={ev[k]}" for k in ("magic", "member", "head", "keys") if k in ev]
                _err(f"{where}: {found.spec or 'unplaceable'}  " + "  ".join(bits))
        return entries, failed

    def process(self, args, rest):
        super().process(args, rest)
        if bool(args.path) == bool(args.source):
            _err("lux guess-dump-type: give one of --source or --path")
            raise SystemExit(2)

        if args.path:
            if not os.path.exists(args.path):
                _err(f"lux guess-dump-type: no such file or directory: {args.path}")
                raise SystemExit(2)
            entries, failed = self.describe(None, args.path, None, args.explain)
            print(f'"input_files": {json.dumps({args.type: entries}, indent=2)}')
            if failed:
                raise SystemExit(1)
            return entries

        if self.configs is None:
            from lux_pipeline.runtime import get_configs
            self.configs = get_configs()
        names = self.resolve_sources(args)
        blocks, failed = {}, 0
        for name in names:
            cfg = self.source_config(name)
            where = self.source_directory(cfg)
            if where is None:
                _err(f"{name}: no dumps directory on disk (from dumps_dir or dumpFilePath); skipped")
                continue
            entries, nfailed = self.describe(name, where[0], where[1], args.explain)
            failed += nfailed
            if entries:
                blocks[name] = {"input_files": {args.type: entries}}
        if len(names) == 1:
            if blocks:
                block = blocks[names[0]]["input_files"]
                print(f'"input_files": {json.dumps(block, indent=2)}')
        elif blocks:
            print(json.dumps(blocks, indent=2))
        if failed:
            raise SystemExit(1)
        return blocks
