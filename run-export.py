"""Export merged records as JSONL, one file per slice: the legacy entry point.

    python run-export.py 0 24             # marklogic documents, raw jsonl
    python run-export.py 0 24 --gz        # gzip -9 instead
    python run-export.py 0 24 --no-ml     # the merged records themselves

LUX_EXPORT_CHUNK (records per marklogic-cache lookup, 1000) and
LUX_EXPORT_GZIP (compression level for --gz, 9) are read from the
environment, as before.

The work is lux_pipeline.process.phases.export.export_slice; this script
parses sys.argv as it always has and calls it. --profile is accepted and
ignored (refactor_docs/41).
"""
import os
import sys
from dotenv import load_dotenv
import logging
logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
from lux_pipeline.config import Config
from lux_pipeline.process.phases.export import export_slice


def main():
    load_dotenv()
    basepath = os.getenv("LUX_BASEPATH", "")
    cfgs = Config(basepath=basepath)
    cfgs.get_idmap()
    cfgs.cache_globals()
    cfgs.instantiate_all()

    # --- process command line arguments ---

    if "--profile" in sys.argv:
        sys.argv.remove("--profile")
    no_ml = "--no-ml" in sys.argv
    gz = "--gz" in sys.argv

    if len(sys.argv) > 2 and sys.argv[1].isnumeric() and sys.argv[2].isnumeric():
        my_slice = int(sys.argv[1])
        max_slice = int(sys.argv[2])
    else:
        my_slice = 0
        max_slice = 1

    chunk_size = int(os.getenv("LUX_EXPORT_CHUNK", "1000"))
    gzip_level = int(os.getenv("LUX_EXPORT_GZIP", "9"))

    export_slice(cfgs, my_slice, max_slice, no_ml=no_ml, gz=gz,
                 chunk_size=chunk_size, gzip_level=gzip_level)


if __name__ == "__main__":
    main()
