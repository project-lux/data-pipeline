"""The `lux` console script: global options, command lookup, config, run.

Importing this module does nothing. `main` parses the global options, sets
up logging, resolves the command module, builds the process's `Config`
through `lux_pipeline.runtime`, and hands over to the command's
`CommandHandler`. Exit status is non-zero on every failure, so shell
orchestration can rely on it.
"""

import importlib
import logging
import multiprocessing
import sys
from argparse import ArgumentParser

from lux_pipeline import runtime

logger = logging.getLogger("lux_pipeline")

ENGINES = ("mp", "ray", "null")
DEFAULT_ENGINE = "mp"
# Commands that may run without a working installation
NO_CONFIG_COMMANDS = ("initialize", "testinstall", "help")


def build_parser():
    ap = ArgumentParser(prog="lux", description="The LUX data pipeline.")
    ap.add_argument("command", help="Command to run; 'lux help' lists them")
    ap.add_argument("--log", help="Log level: DEBUG, INFO, WARNING, ERROR (default INFO)")
    ap.add_argument("--debug", action="store_true", help="Log at DEBUG and re-raise errors")
    ap.add_argument("--source", help="Sources to process: all, internal, external, results, or names separated by commas")
    ap.add_argument("--max-records", "--max_records", type=int, default=-1,
                    help="Stop each worker after this many records")
    ap.add_argument("--max-workers", "--max_workers", type=int, default=0,
                    help="Number of workers (default: max_workers in the base config)")
    ap.add_argument("--my-worker", "--my_worker", type=int, default=-1,
                    help="Run as this worker of --max-workers in this process (engine null)")
    ap.add_argument("--engine", choices=ENGINES, help=f"Task engine (default {DEFAULT_ENGINE})")
    ap.add_argument("--no-ui", "--no_ui", "--disable-ui", action="store_true",
                    help="Plain output instead of the console view")
    ap.add_argument("--prefix", help="Status/log file prefix for this run "
                                     "(default <log_dir>/<command>-<timestamp>)")
    return ap


def _fail(msg, code=1):
    print(msg, file=sys.stderr)
    return code


def resolve_command(name):
    """The module for command `name`, or ``(None, message)``.

    Built-in commands are ``lux_pipeline.cli.<name>`` with ``-`` mapped to
    ``_``. A dotted name is an extension: imported as-is (dashes mapped the
    same way) and required to define `CommandHandler`. That is how a
    deployment adds private commands.
    """
    if not name or name.startswith("_"):
        return None, f"Unknown command '{name}'. See 'lux help' for the list."
    modname = name.replace("-", "_")
    external = "." in name
    if not external:
        modname = f"lux_pipeline.cli.{modname}"
    try:
        mod = importlib.import_module(modname)
    except ModuleNotFoundError as e:
        if e.name == modname or modname.startswith(f"{e.name}."):
            return None, f"Unknown command '{name}'. See 'lux help' for the list."
        return None, f"Failed to import command '{name}': {e}"
    except Exception as e:
        return None, f"Failed to import command '{name}': {e}"
    if not hasattr(mod, "CommandHandler"):
        return None, f"'{name}' is not a command: {modname} has no CommandHandler"
    return mod, None


def run(argv=None):
    """Everything `main` does, returning the exit status instead of exiting."""
    args, rest = build_parser().parse_known_args(argv)

    # Logging first, so everything after (including config loading) is heard
    if args.debug and not args.log:
        args.log = "DEBUG"
    elif not args.log:
        args.log = "INFO"
    try:
        runtime.configure_logging(args.log)
    except ValueError as e:
        return _fail(f"lux: {e}", 2)

    if args.my_worker > -1:
        args.engine = "null"
        if args.max_workers <= args.my_worker:
            return _fail(f"my_worker ({args.my_worker}) must be lower than max_workers "
                         f"({args.max_workers}) -- my_worker starts at 0", 2)
    elif not args.engine:
        args.engine = DEFAULT_ENGINE

    mod, err = resolve_command(args.command)
    if mod is None:
        return _fail(f"lux: {err}", 2)

    cfgs = None
    try:
        # A handler that says needs_configs = False (watch) runs without
        # loading, and so without connecting to anything
        if getattr(mod.CommandHandler, "needs_configs", True):
            cfgs = runtime.get_configs()
    except Exception as e:
        if args.command in NO_CONFIG_COMMANDS:
            logger.debug(f"continuing without a configuration: {e}")
            cfgs = None
        else:
            return _fail(f"lux: could not load the pipeline configuration: {e}\n"
                         "Use 'lux initialize <dir>' to create an installation, "
                         "or 'lux testinstall' to diagnose one.", 1)

    # Workers are spawned, never forked: fork is posix-only and inherits
    # open connections. Raises if already set (e.g. under pytest).
    try:
        multiprocessing.set_start_method("spawn", force=False)
    except RuntimeError:
        pass

    hdlr = mod.CommandHandler(cfgs)
    try:
        if args.debug:
            logger.debug(f"calling {type(hdlr).__name__}.process with {args} and {rest}")
        hdlr.process(args, rest)
    except SystemExit:
        raise
    except Exception as e:
        if args.debug:
            raise
        return _fail(f"lux {args.command}: {type(e).__name__}: {e}", 1)
    return 0


def main():
    sys.exit(run())


if __name__ == "__main__":
    main()
