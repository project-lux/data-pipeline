import code
from rich import pretty
import readline
import rlcompleter
import os

def handle_command(cfgs, args, rest):
    # Nicer print and inspect
    from rich import print, inspect
    pretty.install()

    # Enable command line history
    fn = os.path.expanduser("~/.python_history")
    if os.path.exists(fn):
        readline.read_history_file(fn)

    # Get idmap into locals
    idmap = cfgs.get_idmap()
    vars = globals()
    vars.update(locals())

    # Enable tab completion
    readline.set_completer(rlcompleter.Completer(vars).complete)
    readline.parse_and_bind("bind ^I rl_complete")
    readline.parse_and_bind("tab: complete")

    # Launch the console
    code.InteractiveConsole(vars).interact()
