"""A Python console with the pipeline configuration loaded.

    lux interactive
    echo 'print(len(cfgs.external))' | lux interactive

The namespace holds `cfgs`, `idmap` (``None`` when the map store cannot be
reached), `ref_mgr()` (a ReferenceManager, built on first call because it
reads replacements.json), Rich's `print` and `inspect`, and
`setup_for(task, *sources, **options)`, which returns a one-worker manager
for a command with the sources added, so ``setup_for("load", "ror").run_single()``
runs a load in this process. On a terminal, history is kept in
``<base_dir>/.python_history`` and tab completes names; piped input runs
without prompts.
"""

import atexit
import code
import logging
import os
import sys
from argparse import ArgumentParser

import rlcompleter  # before readline, so the completer binds to the one imported below

from rich import pretty

from ._handler import BaseHandler

try:
    import gnureadline as readline
except ImportError:
    try:
        import readline
    except ImportError:
        readline = None

logger = logging.getLogger("lux_pipeline")

BANNER = "LUX Python Interactive Console: cfgs, idmap, ref_mgr(), setup_for(task, *sources)"


class LUXConsole(code.InteractiveConsole):
    def __init__(self, namespace, prompts=True):
        super().__init__(namespace)
        self.prompts = prompts

    def raw_input(self, prompt=""):
        # piped input: the output is only what the statements print
        return input(prompt if self.prompts else "")


def enable_readline(namespace, history_file):
    """History in `history_file` (written at exit) and tab completion."""
    if readline is None:
        return
    if os.path.exists(history_file):
        try:
            readline.read_history_file(history_file)
        except OSError as e:
            logger.warning(f"could not read {history_file}: {e}")

    def save():
        try:
            readline.write_history_file(history_file)
        except OSError as e:
            logger.warning(f"could not write {history_file}: {e}")

    atexit.register(save)
    readline.set_completer(rlcompleter.Completer(namespace).complete)
    # macOS's stdlib readline is libedit, which has its own binding syntax
    if "libedit" in (readline.__doc__ or ""):
        readline.parse_and_bind("bind ^I rl_complete")
    else:
        readline.parse_and_bind("tab: complete")


class CommandHandler(BaseHandler):
    def setup_for(self, task, *sources, **options):
        """A one-worker manager for `task` (a command name such as
        ``load``) with `sources` added; `options` set the command's
        options, e.g. ``setup_for("load", "ycba", type="export")``.
        Returns ``None`` after printing why when the command has no manager."""
        from lux_pipeline.cli import entry

        mod, err = entry.resolve_command(task)
        if mod is None:
            print(err)
            return None
        ch = mod.CommandHandler(self.configs)
        if not hasattr(ch, "make_manager"):
            print(f"'{task}' does not run a manager")
            return None
        args = entry.build_parser().parse_args([task])
        args.engine, args.log = "null", args.log or "INFO"
        ap = ArgumentParser(prog=f"lux {task}")
        ch.add_args(ap)
        ap.parse_args([], namespace=args)
        for k, v in options.items():
            setattr(args, k, v)
        try:
            mgr = ch.make_manager(1, args)
        except NotImplementedError as e:
            print(e)
            return None
        for s in sources:
            mgr.prepare_single(s)
        return mgr

    def namespace(self):
        from rich import inspect, print

        cfgs = self.configs
        try:
            idmap = cfgs.get_idmap()
        except Exception as e:
            logger.warning(f"idmap unavailable: {e}")
            idmap = None
        ref = []

        def ref_mgr():
            """The ReferenceManager, built on first use."""
            if not ref:
                from lux_pipeline.process.reference_manager import ReferenceManager
                ref.append(ReferenceManager(cfgs, idmap))
            return ref[0]

        return {"__name__": "__console__", "__doc__": None,
                "cfgs": cfgs, "idmap": idmap, "ref_mgr": ref_mgr,
                "setup_for": self.setup_for, "print": print, "inspect": inspect}

    def process(self, args, rest):
        super().process(args, rest)
        pretty.install()
        ns = self.namespace()
        tty = sys.stdin.isatty()
        if tty:
            enable_readline(ns, os.path.join(self.configs.base_dir, ".python_history"))
        LUXConsole(ns, prompts=tty).interact(banner=BANNER if tty else "", exitmsg="")
