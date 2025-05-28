import os
import sys

from ._task_ui_manager import TaskUiManager
from lux_pipeline.cli._rich import get_bar_from_layout

import logging
logger = logging.getLogger("lux_pipeline")


class HarvestManager(TaskUiManager):
    """
    HarvestManager deals with ActivityStreams, OAI-PMH, LDES, and other 'streams' of data or changes
    """

    def _distributed(self, n):
        super()._distributed(n)
        self.idmap = self.configs.get_idmap()

        # this process should fetch every % n stream
        for (which, src, url) in self.sources[n::self.max_workers]:
            try:
                h = getattr(self.configs, which)[src]['harvester']
                h.prepare(self, n, self.max_workers)
                h.process(url, disable_ui=self.disable_ui)
            except Exception as e:
                self.log(logging.ERROR, "Caught Exception:")
                self.log(logging.ERROR, e)
                raise
        return 1

    def maybe_add(self, which, cfg):
        harvester = cfg.get('harvester', None)
        if harvester is not None:
            for coll in cfg['activitystreams']:
                self.sources.append((which, cfg['name'], coll))
        return False

    def process(self, layout, engine="ray", disable_ui=False, **args) -> bool:
        # hide unnecessary bars
        if len(self.sources) < self.max_workers:
            for n in range(len(self.sources), self.max_workers):
                bar = get_bar_from_layout(layout, n)
                bar[0].update(bar[1], visible=False)
        super().process(layout, engine=engine, disable_ui=disable_ui, **args)

