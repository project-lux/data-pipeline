"""Build and export the aat / ulan / lcsh / tgn records: the legacy entry point.

    python run-source-build.py 0 24     # slice 0 of 24
    python run-source-build.py          # unsliced

Each source's datacache is mapped into its recordcache and written out as
JSONL and N-Triples, one file pair per slice. The work is
lux_pipeline.process.phases.export.export_sources_slice; this script parses
sys.argv as it always has and passes the directories it has always written
to (the phase function itself defaults to cfgs.exports_dir).
"""
import os
import sys
from dotenv import load_dotenv
import logging
logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
from lux_pipeline.config import Config
from lux_pipeline.process.phases.export import export_sources_slice

OUT_DIR = "/data-export/output/lux/latest"
NT_DIR = "/data-export/output/lux/nt"


def main():
    load_dotenv()
    basepath = os.getenv("LUX_BASEPATH", "")
    cfgs = Config(basepath=basepath)
    cfgs.get_idmap()
    cfgs.cache_globals()
    cfgs.instantiate_all()

    # --- process command line arguments ---

    my_slice = -1
    max_slice = -1
    if len(sys.argv) > 2 and sys.argv[1].isnumeric() and sys.argv[2].isnumeric():
        my_slice = int(sys.argv[1])
        max_slice = int(sys.argv[2])

    export_sources_slice(cfgs, my_slice, max_slice, out_dir=OUT_DIR, nt_dir=NT_DIR)


if __name__ == "__main__":
    main()
