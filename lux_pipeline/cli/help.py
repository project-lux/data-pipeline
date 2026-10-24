"""List the available lux commands."""

import ast
import logging
import os

from lux_pipeline.cli._handler import BaseHandler

logger = logging.getLogger("lux_pipeline")

GLOBAL_OPTIONS = """\
Global options (before or after the command):
  --log LEVEL          DEBUG, INFO, WARNING or ERROR (default INFO)
  --debug              log at DEBUG and re-raise errors instead of exiting
  --source S           all, internal, external, results, or names separated by commas
  --max-records N      stop each worker after N records
  --max-workers N      number of workers (default: max_workers in the base config)
  --my-worker N        run as worker N of --max-workers in this process (engine null)
  --engine E           mp (default), ray, or null
  --no-ui              plain output instead of the console view
  --prefix P           status/log file prefix for this run (default <log_dir>/<command>-<timestamp>)"""


def list_commands():
    """``[(name, one-line description)]`` for every command module in
    `lux_pipeline.cli`: not ``_``-prefixed, and defining a `CommandHandler`.

    Read with `ast` rather than imported, so listing never has side effects
    and works before an installation exists.
    """
    here = os.path.dirname(os.path.abspath(__file__))
    out = []
    for fn in sorted(os.listdir(here)):
        if not fn.endswith(".py") or fn.startswith("_"):
            continue
        with open(os.path.join(here, fn)) as fh:
            src = fh.read()
        if "class CommandHandler" not in src:
            continue
        try:
            doc = ast.get_docstring(ast.parse(src)) or ""
        except SyntaxError:
            doc = ""
        name = fn[:-3].replace("_", "-")
        out.append((name, doc.strip().split("\n", 1)[0]))
    return out


class CommandHandler(BaseHandler):
    def process(self, args, rest):
        super().process(args, rest)
        logger.debug("listing the command modules in lux_pipeline.cli")
        print("Usage: lux <command> [options]\n")
        print("Commands:")
        for name, doc in list_commands():
            print(f"  {name:<18} {doc}")
        print()
        print(GLOBAL_OPTIONS)
        print("\nA command's own options are listed by: lux <command> --help")
