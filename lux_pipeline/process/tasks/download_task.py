"""`DownloadManager`: fetch every file the selected sources need, spread
over the workers. The manager behind `lux download`.

`maybe_add` asks each source's downloader (item 14) for its files,
``get_urls(download_type)``, and flattens them into ``sources`` as
``(which, name, {"url", "path"})``. Worker `n` takes every
``max_workers``-th entry, so one file is never fetched twice and a source
with several files is spread over several workers. A file that fails is
logged and counted, the worker carries on with its next file, and the
counts come back as the worker's result so the command can exit non-zero
without one bad URL aborting the rest.
"""

import logging

from ._task_manager import TaskManager

logger = logging.getLogger("lux_pipeline")


class DownloadManager(TaskManager):
    """Options, read from `args` in ``__init__``: ``type`` (``all`` or a
    key of ``input_files`` such as ``records`` or ``export``) and
    ``overwrite`` (``--no-overwrite`` clears it; an existing file is then
    kept)."""

    # workers beyond the number of files have nothing to do
    hide_idle_workers = True

    def __init__(self, configs, max_workers=0, args=None):
        super().__init__(configs, max_workers, args)
        self.download_type = "all"
        self.overwrite = True
        self.results = None
        if args is not None:
            dt = getattr(args, "type", None)
            if dt:
                self.download_type = dt
            self.overwrite = bool(getattr(args, "overwrite", True))

    # ----------------------------------------------------------- sources

    def maybe_add(self, which, cfg):
        """One entry per file the source's downloader names for
        ``download_type``; a source without a downloader, or with nothing
        to fetch for this type, adds nothing."""
        dl = cfg.get("downloader")
        if dl is None:
            return
        for u in dl.get_urls(self.download_type) or []:
            key = (which, cfg["name"], u["url"], u["path"])
            if key not in {(w, s, d["url"], d["path"]) for w, s, d in self.sources}:
                self.sources.append((which, cfg["name"], u))

    def my_downloads(self, n):
        """The entries worker `n` fetches."""
        return self.sources[n::self.max_workers]

    def idle_workers(self):
        """Workers with no file to fetch."""
        return range(len(self.sources), self.max_workers)

    # ----------------------------------------------------------- driver

    def process(self, engine="mp", prefix=None, view=None):
        """Refuses to start with nothing to fetch; never starts more
        workers than there are files; keeps the per-worker counts in
        ``results``."""
        if not self.sources:
            raise RuntimeError(f"nothing to download (type {self.download_type!r}) "
                               "for the selected sources")
        if self.max_workers > len(self.sources):
            logger.info(f"{len(self.sources)} file(s) to download: using "
                        f"{len(self.sources)} of {self.max_workers} workers")
            self.max_workers = len(self.sources)
        self.results = super().process(engine, prefix, view)
        return self.results

    def summary(self):
        """``{"downloaded", "kept", "failed"}`` summed over the workers
        that reported."""
        out = {"downloaded": 0, "kept": 0, "failed": 0}
        for r in self.results or []:
            if isinstance(r, dict):
                for k in out:
                    out[k] += r.get(k, 0)
        return out

    # ----------------------------------------------------------- workers

    def _distributed(self, n, cfgs, progress):
        mine = self.my_downloads(n)
        counts = {"downloaded": 0, "kept": 0, "failed": 0}
        if not mine:
            logger.debug(f"worker {n}: nothing to download")
            return counts
        progress(phase="download", total=len(mine), completed=0)
        for which, src, u in mine:
            dl = getattr(cfgs, which)[src]["downloader"]
            dl.prepare(n, self.max_workers, progress=progress)
            try:
                written = dl.process(u, overwrite=self.overwrite)
            except Exception as e:
                logger.error(f"{src}: failed to download {u['url']} to {u['path']}: "
                             f"{type(e).__name__}: {e}", exc_info=True)
                counts["failed"] += 1
                continue
            counts["downloaded" if written else "kept"] += 1
        return counts
