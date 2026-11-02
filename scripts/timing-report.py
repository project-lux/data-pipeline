# Roll up the per-slice timing JSON a build leaves behind.
#
#   python timing-report.py                       # every phase in the log dir
#   python timing-report.py --phase merge
#   python timing-report.py --dir /path/to/logs
#   python timing-report.py --compare old/ new/   # two runs side by side
#   python timing-report.py --profile             # and the top functions (LUX_PROFILE=1 runs)
#
# `lux timing-report` does the same, with the same options; the report itself
# is lux_pipeline.process.timing_report, which both print.

import argparse
import os
import sys

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lux_pipeline.process.timing_report import (  # noqa: E402,F401  (re-exported)
    DEFAULT_TOP, combine, compare_lines, directory_report, func_label, load,
    load_profiles, profile_lines, report_lines, stage_functions, summarise,
    top_functions)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dir", default=None, help="log dir (default: from config)")
    ap.add_argument("--phase", default=None, help="just this phase")
    ap.add_argument("--compare", nargs=2, metavar=("BEFORE", "AFTER"),
                    help="two directories of timing json, side by side")
    ap.add_argument("--profile", nargs="?", const=DEFAULT_TOP, type=int, metavar="N",
                    help="also the top N (default 20) functions from the profile-*.prof "
                         "files a LUX_PROFILE=1 / --profile run leaves")
    args = ap.parse_args()

    if args.compare:
        lines = compare_lines(args.compare[0], args.compare[1], args.phase)
    else:
        directory = args.dir
        if directory is None:
            load_dotenv()
            from lux_pipeline.config import Config
            cfgs = Config(basepath=os.getenv("LUX_BASEPATH", ""))
            directory = cfgs.log_dir
        try:
            lines = directory_report(directory, args.phase, args.profile)
        except (RuntimeError, FileNotFoundError) as e:
            print(f"  {e}")
            sys.exit(1)
    for line in lines:
        print(line)


if __name__ == "__main__":
    main()
