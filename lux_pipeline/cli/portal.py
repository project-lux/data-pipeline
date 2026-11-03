"""Tag the merged records the YPM portal needs, after merge and before export.

    lux portal                     # cpu_count * 2 // 3 processes
    lux portal --max-workers 16

Replaces `post-build-portal.py`; `lux build` runs it as its `portal` phase.
Every merged record within a few references of a YPM record gets
``change = "ypm"`` in the merged recordcache's metadata; see
`lux_pipeline.process.phases.portal`. Refuses while a ``lux`` build is
running (``--force`` overrides): merge is still writing the records it
walks.
"""

from lux_pipeline.process.phases.portal import tag_portal

from ._handler import BaseHandler, refuse_while_building


class CommandHandler(BaseHandler):
    def add_args(self, ap):
        ap.add_argument("--force", action="store_true",
                        help="Run even though a build looks to be running")

    def process(self, args, rest):
        super().process(args, rest)
        refuse_while_building(self.configs, "portal", args.force)
        reached, tagged = tag_portal(self.configs, args.max_workers or None)
        print(f"Portal: {reached} records in reach, {tagged} tagged")
        return reached, tagged
