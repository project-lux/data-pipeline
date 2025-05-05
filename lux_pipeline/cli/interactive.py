import code
from rich import pretty
import readline
import rlcompleter
import os
from ._handler import BaseHandler as BH

class LUXREPL(code.InteractiveConsole):

    def runsource(self, source, filename='<input>', symbol='single'):
        if source.startswith('??'):
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
        if os.path.exists(fn):
            readline.read_history_file(fn)

        # Get idmap into locals
        cfgs = self.configs
        idmap = self.configs.get_idmap()
        vars = globals()
        vars.update(locals())

        # Enable tab completion
        readline.set_completer(rlcompleter.Completer(vars).complete)
        readline.parse_and_bind("bind ^I rl_complete")
        readline.parse_and_bind("tab: complete")

        # Launch the console
        LUXREPL(vars).interact(banner="LUX Python Interactive Console")

