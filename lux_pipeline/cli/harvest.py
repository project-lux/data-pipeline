"""Harvest each source's change feed into its caches.

    lux harvest --source ycba                               # walk the stream since the last harvest
    lux harvest --source all --two-phase --max-workers 12   # list in phase 1, fetch records in parallel in phase 2
    lux harvest --source ypm --until 2025-01-01T00:00:00
    lux harvest --source ycba --pages --max-workers 24      # fetch feed pages into the page cache

Replaces `run-harvest.py` and `harvest_parallel.sh`. `--phase 1` or
`--phase 2` runs one half of a two-phase harvest, for a shell loop on the
null engine. A source that fails is logged and the rest still run; the
command exits non-zero at the end if any failed.
"""

import logging
from datetime import datetime

from lux_pipeline.process.tasks.harvest_task import HarvestManager

from ._handler import CommandHandler as CH

logger = logging.getLogger("lux_pipeline")


def iso8601(value):
    try:
        datetime.fromisoformat(value)
    except ValueError:
        raise ValueError(f"not an ISO 8601 date/time: {value!r}") from None
    return value


class CommandHandler(CH):
    def add_args(self, ap):
        ap.add_argument("--two-phase", "--two_phase", dest="two_phase", action="store_true",
                        help="List the records from the feed first (one worker per source), "
                             "then fetch them with every worker")
        ap.add_argument("--phase", type=int, choices=(1, 2),
                        help="Run only phase 1 (list) or phase 2 (fetch) of a two-phase harvest")
        ap.add_argument("--pages", action="store_true",
                        help="Only fetch feed pages into the page cache, every worker taking "
                             "its share of the pages")
        ap.add_argument("--until", type=iso8601,
                        help="Walk the feed back to this ISO 8601 time instead of the last harvest")
        ap.add_argument("--no-overwrite", dest="overwrite", action="store_false",
                        help="Keep records already in the datacache")
        self.extra_args = {"two_phase": "two_phase", "phase": "phase", "pages": "pages",
                           "until": "until", "overwrite": "overwrite"}

    def make_manager(self, wks, args):
        return HarvestManager(self.configs, wks, args)

    def process(self, args, rest):
        manager = super().process(args, rest)
        if manager is None:
            return None
        s = manager.summary()
        logger.info(f"harvested {s['records']} record(s) over {s['sources']} source run(s), "
                    f"{s['failed']} failed")
        if s["failed"]:
            raise RuntimeError(f"{s['failed']} harvest(s) failed; see the worker logs")
        return manager
