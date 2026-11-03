"""Download the files each source's loader reads.

    lux download --source all --type records --max-workers 10
    lux download --source wikidata                 # one file, one worker
    lux download --source all --type export        # the published LUX export set
    lux download --source ror --no-overwrite       # keep what is already there

Which files a source needs comes from its downloader (`remoteDumpFile` +
`dumpFilePath`, or the `input_files` block; see
`lux_pipeline.process.base.downloader`). Files are spread over the
workers; a file that fails is logged and the rest still download, and the
command exits non-zero at the end if any failed.
"""

import logging

from lux_pipeline.process.tasks.download_task import DownloadManager

from ._handler import CommandHandler as CH

logger = logging.getLogger("lux_pipeline")


class CommandHandler(CH):
    def add_args(self, ap):
        ap.add_argument("--type", type=str, default="all",
                        help="Which files: 'all', or a key of input_files such as "
                             "records or export (default all)")
        ap.add_argument("--no-overwrite", dest="overwrite", action="store_false",
                        help="Keep files that are already downloaded")
        self.extra_args = {"download_type": "type", "overwrite": "overwrite"}

    def make_manager(self, wks, args):
        return DownloadManager(self.configs, wks, args)

    def process(self, args, rest):
        manager = super().process(args, rest)
        if manager is None:
            return None
        s = manager.summary()
        logger.info(f"downloaded {s['downloaded']}, kept {s['kept']}, failed {s['failed']}")
        if s["failed"]:
            raise RuntimeError(f"{s['failed']} download(s) failed; see the worker logs")
        return manager
