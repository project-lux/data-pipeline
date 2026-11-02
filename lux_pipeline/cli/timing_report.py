"""Add up the build's per-slice timing, stage by stage, and its profiles.

    lux timing-report                          # every phase in log_dir
    lux timing-report --phase merge
    lux timing-report --dir /path/to/logs
    lux timing-report --compare old/ new/      # two runs side by side
    lux timing-report --profile                # and the top 20 functions (--profile runs)
    lux timing-report --profile 40 --phase reconcile

`timing-report.py` as a command. Every phase's `PhaseTimer` writes
``timing-<phase>-<slice>.json`` to ``log_dir``; this adds each phase's
slices up: records, wall time, worker- and cpu-hours, the share of worker
time in each stage (nested stages listed under their parent, never
subtracted twice), and the slow and fast slices. Works on a running build
too: a phase still in flight shows as not finished, with partial numbers.

``--profile [N]`` adds the top N functions from the
``profile-<phase>-<slice>.prof`` files that ``lux reconcile|merge|export
--profile`` (or ``LUX_PROFILE=1``) leaves beside the JSON, by cumulative
and by own time, and what each stage's same-named function calls.

``--dir`` (and ``--compare``) read the given directories without loading
the configuration; otherwise the configuration is read, not instantiated,
for ``log_dir``. Read-only, so safe while a build runs.
"""

from lux_pipeline import runtime
from lux_pipeline.process import timing_report

from ._handler import BaseHandler


class CommandHandler(BaseHandler):
    # only log_dir is needed, and only without --dir: never open the idmap
    needs_configs = False

    def add_args(self, ap):
        ap.add_argument("--dir", help="Directory of timing-*.json (default: log_dir)")
        ap.add_argument("--phase", help="Only this phase, e.g. merge or reconcile-refs")
        ap.add_argument("--compare", nargs=2, metavar=("BEFORE", "AFTER"),
                        help="Two directories of timing JSON, side by side")
        ap.add_argument("--profile", nargs="?", const=timing_report.DEFAULT_TOP, type=int,
                        metavar="N",
                        help=f"Also the top N (default {timing_report.DEFAULT_TOP}) functions "
                             "from the profile-*.prof files a --profile run leaves")

    def process(self, args, rest):
        super().process(args, rest)
        if args.compare:
            lines = timing_report.compare_lines(args.compare[0], args.compare[1], args.phase)
        else:
            directory = args.dir
            if directory is None:
                cfgs = self.configs or runtime.get_configs(instantiate=False)
                directory = cfgs.log_dir
            lines = timing_report.directory_report(directory, args.phase, args.profile)
        # the report is the command's result
        for line in lines:
            print(line)
        return lines
