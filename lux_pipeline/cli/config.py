"""Show, check and edit the configuration records in their files.

    lux config list                          # every record: key, type, name, store, file
    lux config show aat                      # the record's file as it is
    lux config show aat --path               # only where it is
    lux config check                         # every record; exit 1 if any has an error
    lux config check aat ycba
    lux config set aat merge_order 12        # a JSON value, or bare text for a string
    lux config set aat indexes.reconcile_label.path label_to_aat.lmdb
    lux config unset aat cycleBreakPath
    lux config edit aat                      # $VISUAL / $EDITOR on the file, checked before it is saved
    lux config diff aat --file new.json      # what writing new.json would change (or on stdin)
    lux config new wd2 --type external       # print a template to fill in
    lux config new wd2 --store sources --file wd2.json
    lux config delete wd2                    # asks for the key to be typed; --force does not

Records are read from and written to the stores they came from: the
configuration directory, or the sub-config store that held them
(``cfgs.config_store``, `lux_pipeline.config_store`). They are written as
normalized JSON (2-space indent), whatever the file looked like before.
Before anything is written the record is checked: against the JSON Schema
generated from the catalogue (`lux_pipeline.config_schema`: its type, the
keys it needs, the types of the keys the catalogue knows), then that every
``*Class`` imports, a source's name is its key, and map names are unique;
a class path from before the package rename is a warning, as is a record
that replaces one of the same key in an earlier store. ``set``,
``unset``, ``edit``, ``new`` and ``delete`` refuse while a build runs (its
workers read the configuration as they start); ``--force`` writes anyway.
The configuration is read, never instantiated: nothing connects to a
database.
"""

import json
import os
import shlex
import subprocess
import sys
import tempfile

from lux_pipeline import config_schema, runtime
from lux_pipeline import config_store as store
from lux_pipeline.config_store import ConfigError

from ._handler import BaseHandler, refuse_while_building

ACTIONS = ("list", "show", "check", "set", "unset", "edit", "diff", "new", "delete")
WRITES = ("set", "unset", "edit", "new", "delete")


class CommandHandler(BaseHandler):
    # read, never instantiated: no database is needed to edit a record
    needs_configs = False

    def add_args(self, ap):
        ap.add_argument("action", choices=ACTIONS, help="What to do")
        ap.add_argument("items", nargs="*", metavar="KEY [PATH VALUE]",
                        help="The record key; for set, the key path and the value; for check, "
                             "any number of keys")
        ap.add_argument("--path", dest="only_path", action="store_true",
                        help="show: print only the record's file")
        ap.add_argument("--type", dest="record_type", choices=config_schema.RECORD_TYPES,
                        help="new: the record's type (for the template)")
        ap.add_argument("--store", help="new: the sub-config store to put the record in "
                                        "(default: the configuration directory)")
        ap.add_argument("--file", dest="input_file",
                        help="diff, new: read the record from this file instead of stdin")
        ap.add_argument("--force", action="store_true",
                        help="Write even though a build looks to be running; delete without "
                             "asking")

    # --- helpers -------------------------------------------------------------------

    def configs_for(self):
        return self.configs or runtime.get_configs(instantiate=False)

    @staticmethod
    def one_key(args, action, extra=0):
        items = args.items or []
        if len(items) != 1 + extra:
            want = {0: "KEY", 1: "KEY PATH", 2: "KEY PATH VALUE"}[extra]
            raise ValueError(f"lux config {action} takes {want}"
                             + (" (quote a value with spaces)" if extra == 2 else ""))
        return items

    @staticmethod
    def read_input(args, required=True):
        """The JSON text given with --file or on stdin; None when neither
        has anything (and it is not `required`)."""
        if args.input_file:
            with open(args.input_file, encoding="utf-8") as fh:
                return fh.read()
        try:
            text = None if sys.stdin is None or sys.stdin.isatty() else sys.stdin.read()
        except (OSError, ValueError):
            text = None      # no stdin to read (a captured or closed one)
        if text and text.strip():
            return text
        if required:
            raise ValueError("give the record with --file or on stdin")
        return None

    @staticmethod
    def parse(text):
        try:
            return json.loads(text)
        except ValueError as e:
            raise ValueError(f"not JSON: {e}") from None

    @staticmethod
    def say_written(path, warnings):
        for w in warnings:
            print(w)
        print(f"wrote {path}")

    def write(self, cfgs, key, data):
        diff = cfgs.config_store.diff(key, data)
        if not diff:
            print("no change")
            return None
        path, warnings = cfgs.config_store.write(key, data)
        sys.stdout.write(diff)
        self.say_written(path, warnings)
        return path

    # --- the actions ------------------------------------------------------------------

    def do_list(self, cfgs, args):
        rows = cfgs.config_store.records()
        cols = [("KEY", "key"), ("TYPE", "type"), ("NAME", "name"), ("STORE", "store")]
        widths = [max([len(h)] + [len(str(r[k] or "")) for r in rows]) for h, k in cols]
        print("  ".join(h.ljust(w) for (h, _), w in zip(cols, widths)) + "  FILE")
        for r in rows:
            cells = [str(r[k] or "-").ljust(w) for (_, k), w in zip(cols, widths)]
            print("  ".join(cells) + "  " + (r["path"] or "-") + (f"  ({r['error']})" if r["error"] else ""))
        return rows

    def do_show(self, cfgs, args):
        (key,) = self.one_key(args, "show")
        if args.only_path:
            print(cfgs.config_store.path(key))
            return cfgs.config_store.path(key)
        text = cfgs.config_store.text(key)
        sys.stdout.write(text if text.endswith("\n") else text + "\n")
        return text

    def do_check(self, cfgs, args):
        keys = args.items or list(cfgs.record_origins)
        errors = warnings = 0
        found = {}
        for key in keys:
            problems = cfgs.config_store.check(key) if key in cfgs.record_origins else \
                [str(cfgs.config_store.unknown(key))]
            found[key] = problems
            for p in problems:
                print(f"{key}: {p}")
                if p.startswith("warning:"):
                    warnings += 1
                else:
                    errors += 1
        print(f"{len(keys)} record(s) checked: {errors} error(s), {warnings} warning(s)")
        if errors:
            raise RuntimeError(f"{errors} error(s) in the configuration")
        return found

    def do_set(self, cfgs, args):
        key, path, value = self.one_key(args, "set", 2)
        data = store.set_key(cfgs.config_store.read(key), path, store.parse_value(value))
        return self.write(cfgs, key, data)

    def do_unset(self, cfgs, args):
        key, path = self.one_key(args, "unset", 1)
        data = store.unset_key(cfgs.config_store.read(key), path)
        return self.write(cfgs, key, data)

    def edit_file(self, path):
        """Run the editor on `path` (``$VISUAL``, ``$EDITOR``, else vi)."""
        editor = os.environ.get("VISUAL") or os.environ.get("EDITOR") or "vi"
        subprocess.run([*shlex.split(editor), path], check=True)

    def do_edit(self, cfgs, args):
        (key,) = self.one_key(args, "edit")
        text = cfgs.config_store.text(key)
        fd, tmp = tempfile.mkstemp(prefix=f"lux-config-{key}-", suffix=".json")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        self.edit_file(tmp)
        with open(tmp, encoding="utf-8") as fh:
            edited = fh.read()
        if edited == text:
            os.remove(tmp)
            print("no change")
            return None
        try:
            path, warnings = cfgs.config_store.write_text(key, edited)
        except ConfigError as e:
            raise ConfigError(f"{e}. Your edit is kept in {tmp}") from None
        os.remove(tmp)
        self.say_written(path, warnings)
        return path

    def do_diff(self, cfgs, args):
        (key,) = self.one_key(args, "diff")
        data = self.parse(self.read_input(args))
        diff = cfgs.config_store.diff(key, data)
        sys.stdout.write(diff or "no change\n")
        for p in cfgs.config_store.validate(key, data):
            print(p if p.startswith("warning:") else f"error: {p}")
        return diff

    def do_new(self, cfgs, args):
        (key,) = self.one_key(args, "new")
        text = self.read_input(args, required=False)
        if text is None:
            typ = args.record_type
            if not typ:
                raise ValueError("give --type for a template, or the record with --file or on stdin")
            data = config_schema.template(typ, key)
            print(cfgs.config_store.dumps(data, args.store), end="")
            missing = store.errors_only(cfgs.config_store.validate(key, data, new=True, store=args.store))
            if missing:
                print("Fill in: " + "; ".join(missing), file=sys.stderr)
            print(f"Then: lux config new {key}"
                  + (f" --store {args.store}" if args.store else "") + " --file <it>",
                  file=sys.stderr)
            return data
        data = self.parse(text)
        if args.record_type and isinstance(data, dict) and data.get("type") != args.record_type:
            raise ValueError(f"--type {args.record_type}, but the record says {data.get('type')!r}")
        path, warnings = cfgs.config_store.create(key, data, store=args.store)
        self.say_written(path, warnings)
        return path

    def confirm(self, key, path):
        if not sys.stdin.isatty():
            raise RuntimeError(f"not deleting {key} without --force (no terminal to ask on)")
        answer = input(f"Delete {path}? Type the key ({key}) to confirm: ")
        return answer.strip() == key

    def do_delete(self, cfgs, args):
        (key,) = self.one_key(args, "delete")
        path = cfgs.config_store.path(key)
        if not args.force and not self.confirm(key, path):
            print("not deleted")
            return None
        cfgs.config_store.delete(key)
        print(f"deleted {path}")
        return path

    def process(self, args, rest):
        super().process(args, rest)
        cfgs = self.configs_for()
        if args.action in WRITES:
            refuse_while_building(cfgs, "config", args.force)
        return getattr(self, f"do_{args.action}")(cfgs, args)
