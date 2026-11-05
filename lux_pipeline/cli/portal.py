"""Tag the merged records the portals need, after merge and before export.

    lux portal                     # cpu_count * 2 // 3 processes
    lux portal --max-workers 16

Replaces `post-build-portal.py`; `lux build` runs it as its `portal` phase.
A portal is an internal source whose configuration has ``"portal": true``
(tagged with the source's name) or ``"portal": "<tag>"``. Every merged
record within a few references of a portal source's records gets the tag
in the merged recordcache's ``change`` metadata (several tags joined with
``|``); see `lux_pipeline.process.phases.portal`. With no portal source
configured it tags nothing. Refuses while a ``lux`` build is
running (``--force`` overrides): merge is still writing the records it
walks.
"""

from lux_pipeline.process.phases.portal import portal_sources, tag_portal

from ._handler import BaseHandler, refuse_while_building


class CommandHandler(BaseHandler):
    def add_args(self, ap):
        ap.add_argument("--force", action="store_true",
                        help="Run even though a build looks to be running")

    def process(self, args, rest):
        super().process(args, rest)
        refuse_while_building(self.configs, "portal", args.force)
        sources = portal_sources(self.configs)
        if not sources:
            print("Portal: no source has 'portal' set in its configuration; nothing to tag")
            return 0, 0
        print("Portal: " + ", ".join(f"{s} -> {t}" for s, t in sorted(sources.items())))
        reached, tagged = tag_portal(self.configs, args.max_workers or None, sources)
        print(f"Portal: {reached} records in reach, {tagged} tagged")
        return reached, tagged
