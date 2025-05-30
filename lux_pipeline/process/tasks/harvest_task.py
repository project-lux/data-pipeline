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

    def _distributed_phase1(self, n):
        for which, src in self.sources[n :: self.max_workers]:
            try:
                h = getattr(self.configs, which)[src]["harvester"]
                h.prepare(self, n, self.max_workers)
                h.get_record_list()
            except Exception as e:
                self.log(logging.ERROR, "Caught Exception:")
                self.log(logging.ERROR, e)
                raise

    def _distributed_phase2(self, n):
        for which, src in self.sources:
            # Each worker reads every file from every source
            # but only retrieves n::max records

            try:
                h = getattr(self.configs, which)[src]["harvester"]
                h.prepare(self, n, self.max_workers)
                h.divide_by_max_slice = True
                h.harvest_from_list(n, self.max_workers)
            except Exception as e:
                self.log(logging.ERROR, "Caught Exception:")
                self.log(logging.ERROR, e)
                raise

    def _distributed(self, n):
        super()._distributed(n)
        self.idmap = self.configs.get_idmap()

        if self.two_phase:
            if self.phase == 1:
                self.log(logging.INFO, f"Entering phase 1 in {n}")
                self._distributed_phase1(n)
            elif self.phase == 2:
                self.log(logging.INFO, f"Entering phase 2 in {n}")
                self._distributed_phase2(n)
            else:
                raise ValueError("Invalid phase")
        else:
            # this process should fetch every % n stream and fetch records
            for which, src in self.sources[n :: self.max_workers]:
                try:
                    h = getattr(self.configs, which)[src]["harvester"]
                    h.prepare(self, n, self.max_workers)
                    h.process(disable_ui=self.disable_ui)
                except Exception as e:
                    self.log(logging.ERROR, "Caught Exception:")
                    self.log(logging.ERROR, e)
                    raise
        return 1

    def maybe_add(self, which, cfg):
        harvester = cfg.get("harvester", None)
        if harvester is not None and cfg.get("activitystreams", None):
            self.sources.append((which, cfg["name"]))
        return False

    def process(self, layout, engine="ray", disable_ui=False, **args) -> bool:
        # hide unnecessary bars

        if len(self.sources) < self.max_workers:
            for n in range(len(self.sources), self.max_workers):
                bar = get_bar_from_layout(layout, n)
                bar[0].update(bar[1], visible=False)

        if "until" in args:
            self.until = args["until"]
        else:
            self.until = "0000"

        if "two_phase" in args and args["two_phase"]:
            # do two phase harvest of fetch URIs sequentially then records in parallel
            self.two_phase = True
            self.phase = 1

            # Find the records from the stream
            # Here we're calling once per source
            mw = self.max_workers
            self.max_workers = len(self.sources)

            logger.info("Entering Phase 1 in master")
            super().process(layout, **args)
            logger.info("Back from phase 1")
            self.max_workers = mw
            self.engine.max_workers = mw

            # re-show all the bars
            for n in range(len(self.sources), self.max_workers):
                bar = get_bar_from_layout(layout, n)
                bar[0].update(bar[1], visible=True)

            # Harvest the records
            # Here we're calling as many as possible to step through the record lists
            self.phase = 2
            self.engine.process(layout)

        else:
            self.two_phase = False
            self.phase = 0
            super().process(layout, engine=engine, disable_ui=disable_ui, **args)
