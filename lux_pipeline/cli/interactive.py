import code
from rich import pretty
import os
from ._handler import BaseHandler as BH
import importlib

try:
    import readline
    import rlcompleter
except Exception:
    readline = None


class LUXREPL(code.InteractiveConsole):
    def runsource(self, source, filename="<input>", symbol="single"):
        if source.startswith("??"):
            print(f"Intercept question: {source[2:].strip()}")
        else:
            return super().runsource(source, filename, symbol)


class CommandHandler(BH):
    def process(self, args, rest):
        # Nicer print and inspect
        from rich import print, inspect

        pretty.install()

        # Enable command line history
        fn = os.path.expanduser("~/.python_history")
        if readline is not None and os.path.exists(fn):
            readline.read_history_file(fn)

        def setup_for(task):
            # make the manager and the task
            try:
                mod = importlib.import_module(f"lux_pipeline.cli.{task}")
            except Exception:
                print(f"Failed to import module for task: {task}")
                return
            ch = mod.CommandHandler(self.configs)
            return ch.make_manager(1, None)

        # Get idmap into locals
        cfgs = self.configs
        idmap = self.configs.get_idmap()
        vars = globals()
        vars.update(locals())

        # Enable tab completion
        if readline is not None:
            readline.set_completer(rlcompleter.Completer(vars).complete)
            if "libedit" in readline.__doc__:
                readline.parse_and_bind("bind ^I rl_complete")
            else:
                readline.parse_and_bind("tab: complete")

        # Launch the console
        LUXREPL(vars).interact(banner="LUX Python Interactive Console")
