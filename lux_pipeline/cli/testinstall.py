import os
from dotenv import load_dotenv, find_dotenv
from ..config import Config
from ._handler import BaseHandler as BH

class CommandHandler(BH):
    def process(self, args, rest):
        cfgs = self.configs
        if cfgs is None:
            # Okay, something went wrong with building the environment
            basepath = os.getenv("LUX_BASEPATH", "")
            idmap = None

            fn = find_dotenv(usecwd=True)
            if not fn and not basepath:
                print("Could not find a .env file, and LUX_BASEPATH not set in environment")
                return
            elif fn:
                load_dotenv(fn)
                basepath = os.getenv("LUX_BASEPATH", "")
                if not basepath:
                    print(f"Found .env at {fn} but it didn't set LUX_BASEPATH")
                    return
            try:
                cfgs = Config(basepath=basepath)
            except Exception as e:
                print(f"Couldn't build Config at from files at {basepath}")
                raise
                return

            try:
                idmap = cfgs.get_idmap()
            except Exception as e:
                print(f"Failed to build an idmap")
                print(e)
                return

            try:
                cfgs.cache_globals()
            except Exception as e:
                print(f"Failed to build globals")
                print(e)
                return

            try:
                cfgs.instantiate_all()
            except Exception as e:
                print(f"Failed to build all...")
                print(e)
                return

            # Try and load everything up individually

            # Can I see the caches?

            # Can I see the maps?
            print("Configs wasn't built, but I can't find anything wrong :(")

        else:
            # Nope, at least the basics are fine
            print("Configs was built, everything seems okay")

