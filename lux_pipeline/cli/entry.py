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
import os
import sys
from argparse import ArgumentParser, RawDescriptionHelpFormatter

from lux_pipeline import runtime
from lux_pipeline.cli._handler import (PARENT_RUN_ENV, command_argv, ensure_driver_log, exit_code,
                                       finish_record, run_extra)
from lux_pipeline.progress import RunRecord

logger = logging.getLogger("lux_pipeline")

ENGINES = ("mp", "ray", "null")
DEFAULT_ENGINE = "mp"
# set in a process `--background` started (`_background.child_env`)
BACKGROUND_CHILD_ENV = "LUX_BACKGROUND_CHILD"
# Commands that may run without a working installation
NO_CONFIG_COMMANDS = ("initialize", "testinstall", "help")

# other names for a command: `lux install` is `lux initialize`. Resolved
# before anything else reads the name, so the run is recorded under the
# command's own name
ALIASES = {"install": "initialize"}


def build_parser():
    # --help is ours to handle: argparse's would print these global options
    # and exit before the command's own options were ever added
    ap = ArgumentParser(prog="lux", description="The LUX data pipeline.", add_help=False)
    ap.add_argument("-h", "--help", action="store_true",
                    help="Show the options of the command (or, alone, these)")
    ap.add_argument("command", nargs="?", help="Command to run; 'lux help' lists them")
    ap.add_argument("--log", help="Log level: DEBUG, INFO, WARNING, ERROR (default INFO)")
    ap.add_argument("--debug", action="store_true", help="Log at DEBUG and re-raise errors")
    ap.add_argument("--env", metavar="NAME|FILE|DIR",
                    help="Use this .env instead of ./.env and ~/.lux/.env: a file, a "
                         "directory's .env, or ~/.lux/<NAME>.env")
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
    ap.add_argument("--prefix", help="This run's directory: its record, logs and workers' status "
                                     "files (default <log_dir>/<command>-<timestamp>)")
    ap.add_argument("-b", "--background", action="store_true",
                    help="Start the command in the background and return: output in "
                         "<prefix>/stdout.log, follow it with lux watch <prefix>")
    return ap


def _fail(msg, code=1):
    print(msg, file=sys.stderr)
    return code


def resolve_command(name):
    """The module for command `name`, or ``(None, message)``.

    Built-in commands are ``lux_pipeline.cli.<name>`` with ``-`` mapped to
    ``_``; a name in `ALIASES` is the command it stands for. A dotted name is an extension: imported as-is (dashes mapped the
    same way) and required to define `CommandHandler`. That is how a
    deployment adds private commands.
    """
    if not name or name.startswith("_"):
        return None, f"Unknown command '{name}'. See 'lux help' for the list."
    name = ALIASES.get(name, name)
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


def command_help(name):
    """The help for command `name`: its docstring, its own options and
    the global options, or ``(None, message)`` for an unknown command.
    Built without a configuration, so it works before an installation
    exists and loads nothing."""
    mod, err = resolve_command(name)
    if mod is None:
        return None, err
    from lux_pipeline.cli.help import GLOBAL_OPTIONS
    ap = ArgumentParser(prog=f"lux {name}", description=(mod.__doc__ or "").strip(),
                        formatter_class=RawDescriptionHelpFormatter, add_help=False,
                        epilog=GLOBAL_OPTIONS)
    mod.CommandHandler(None).add_args(ap)
    return ap.format_help(), None


def run(argv=None):
    """Everything `main` does, returning the exit status instead of exiting."""
    ap = build_parser()
    args, rest = ap.parse_known_args(argv)
    args.command = ALIASES.get(args.command, args.command)
    # the command line as given, for the run record (<prefix>/run.json)
    args.cli_argv = list(sys.argv[1:] if argv is None else argv)
    if args.help:
        if not args.command:
            ap.print_help()
            return 0
        text, err = command_help(args.command)
        if text is None:
            return _fail(f"lux: {err}", 2)
        print(text)
        return 0
    if not args.command:
        return _fail("lux: no command given. See 'lux help' for the list.", 2)

    # Logging first, so everything after (including config loading) is heard
    if args.debug and not args.log:
        args.log = "DEBUG"
    elif not args.log:
        args.log = "INFO"
    try:
        runtime.configure_logging(args.log)
    except ValueError as e:
        return _fail(f"lux: {e}", 2)

    if args.env:
        try:
            fn = runtime.use_env(args.env)
        except (FileNotFoundError, ValueError) as e:
            return _fail(f"lux: {e}", 2)
        logger.debug(f"environment from {fn}")

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

    if args.background:
        if not os.environ.get(BACKGROUND_CHILD_ENV):
            return start_background(mod, args)
        # a started command never starts itself again
        logger.warning("--background ignored: this is the background process")

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
    # Given --prefix, the run has files to follow: the driver's log, and a
    # record for a command that writes none of its own (a one-shot step of
    # a build, a command the web UI started), finished here on the way out
    own = None
    if args.prefix and getattr(hdlr, "uses_prefix", True):
        ensure_driver_log(args.prefix, args.log)
        if not getattr(hdlr, "own_record", False):
            own = start_record(hdlr, args)
    try:
        if args.debug:
            logger.debug(f"calling {type(hdlr).__name__}.process with {args} and {rest}")
        hdlr.process(args, rest)
    except BaseException as e:
        # a command may fail after its workers succeeded and its record was
        # finished (harvest with failed sources, validate --strict): the
        # record says how the command ended, not how the workers did
        if not (isinstance(e, SystemExit) and exit_code(e) == 0):
            finish_record(getattr(hdlr, "run_record", None), e, getattr(hdlr, "run_prefix", None))
        elif own is not None:
            finish_record(own)
        # as before: only an Exception becomes a one-line error and exit 1
        if not isinstance(e, Exception) or args.debug:
            raise
        return _fail(f"lux {args.command}: {type(e).__name__}: {e}", 1)
    if own is not None:
        finish_record(own)
    return 0


def start_background(mod, args):
    """``--background``: start this command line again as a process of its
    own (`_background.start`) and return. Needs only the log directory, so
    the configuration is read, not instantiated."""
    from lux_pipeline.cli import _background
    if not getattr(mod.CommandHandler, "uses_prefix", True):
        return _fail(f"lux {args.command}: --background does not apply: it is not a run", 2)
    try:
        cfgs = runtime.get_configs(instantiate=False)
    except Exception as e:
        if args.command not in NO_CONFIG_COMMANDS:
            return _fail(f"lux: could not load the pipeline configuration: {e}", 1)
        cfgs = None
    try:
        return _background.start(args, cfgs)
    except OSError as e:
        return _fail(f"lux {args.command}: could not start it in the background: {e}", 1)


def start_record(hdlr, args):
    """``<prefix>/run.json`` for a command with no workers and no record of its
    own, so it is a run like the others (`lux_pipeline.progress.list_runs`);
    set on the handler so a failure finishes it. Never raises."""
    try:
        record = RunRecord(args.prefix, args.command, command_argv(args), engine=None, workers=0,
                           parent=os.environ.get(PARENT_RUN_ENV) or None, extra=run_extra()).start()
    except Exception as e:
        logger.debug(f"could not write the run record for {args.prefix}: {e}")
        return None
    hdlr.run_record = record
    hdlr.run_prefix = args.prefix
    return record


def main():
    sys.exit(run())


if __name__ == "__main__":
    main()
