"""Mint a new idmap update token for the next build.

    lux new-token              # before reconciling a rebuild

`manage-data.py --new-token` as a command (``lux reconcile --new-token``
does the same before its workers start): writes ``__YYYYMMDD__``, or
``__YYYYMMDDa__``, ``b``, ... for a later build the same day, to
``<data_dir>/idmap_update_token.txt``. Every idmap reads the token when it
is built and the Postgres one refuses to start without it, so a rebuild
needs a new one before reconcile.

Works without the identity map, and so without a database: the current
token is read from the file, and a missing or malformed file -- which
would stop the idmap, and every other command, from starting -- gives
today's first token. Refuses while a ``lux`` build is running (``--force``
overrides): a new token mid-build splits one build across two.
"""

from lux_pipeline import runtime
from lux_pipeline.process.phases.reconcile import new_update_token

from ._handler import BaseHandler, refuse_while_building


class CommandHandler(BaseHandler):
    # the configuration is loaded here without instantiating it: building the
    # idmap needs the very file this command writes
    needs_configs = False

    def add_args(self, ap):
        ap.add_argument("--force", action="store_true",
                        help="Run even though a build looks to be running")

    def process(self, args, rest):
        super().process(args, rest)
        cfgs = self.configs or runtime.get_configs(instantiate=False)
        refuse_while_building(cfgs, "new-token", args.force)
        # logs "New update token: <token>"
        return new_update_token(cfgs)
