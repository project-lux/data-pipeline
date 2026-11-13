"""Add up the build's per-slice timing, stage by stage, and its profiles.

    lux timing-report                          # each phase's latest run in log_dir
    lux timing-report --phase merge
    lux timing-report --dir <log_dir>/build-<ts>   # one run, or a build's steps
    lux timing-report --dir /path/to/logs
    lux timing-report --compare old/ new/      # two runs side by side
    lux timing-report --profile                # and the top 20 functions (--profile runs)
    lux timing-report --profile 40 --phase reconcile
    lux --no-ui timing-report                  # plain text, as timing-report.py prints it

`timing-report.py` as a command. Every phase's `PhaseTimer` writes
``timing-<phase>-<slice>.json`` into its run's directory
(``<log_dir>/<command>-<timestamp>/``; ``log_dir`` itself for a run
with no prefix); this finds each phase's latest run in the directory
given and the runs below it, and adds that run's slices up: records,
wall time, worker- and cpu-hours, the share of worker time in each stage (nested stages listed under their parent, never
subtracted twice), and the slow and fast slices. Works on a running build
too: a phase still in flight shows as not finished, with partial numbers.
Phases are listed in the order the build runs them (reconcile,
reconcile-refs, identify, merge, ...).

With more than one phase, a summary of them all follows: start to end
and the time between phases, a timeline, the stages that account for
the most elapsed time across every phase, and the phases that waited on
a slow slice.

``--profile [N]`` adds the top N functions from the
``profile-<phase>-<slice>.prof`` files that ``lux reconcile|merge|export
--profile`` (or ``LUX_PROFILE=1``) leaves beside the JSON, by cumulative
and by own time, and what each stage's same-named function calls.

``--dir`` (and ``--compare``) take ``log_dir``, a run's or a build's
directory, or an old flat copy, and read them without loading
the configuration; otherwise the configuration is read, not instantiated,
for ``log_dir``. Read-only, so safe while a build runs.

Rendered with Rich: a panel per phase, a bar for each stage's share of
worker time, slow slices in red, and in ``--compare`` changes coloured by
whether they are better or worse. ``--no-ui`` prints the plain text the
root ``timing-report.py`` does, for logs and diffs.
"""

from lux_pipeline import runtime
from lux_pipeline.process.utils import timing_report

from ._handler import BaseHandler


class CommandHandler(BaseHandler):
    # only log_dir is needed, and only without --dir: never open the idmap
    needs_configs = False

    def add_args(self, ap):
        ap.add_argument("--dir", help="A run's or build's directory, or one holding runs or "
                        "timing-*.json (default: log_dir, each phase's latest run)")
        ap.add_argument("--phase", help="Only this phase, e.g. merge or reconcile-refs")
        ap.add_argument("--compare", nargs=2, metavar=("BEFORE", "AFTER"),
                        help="Two runs' or builds' directories, side by side")
        ap.add_argument("--profile", nargs="?", const=timing_report.DEFAULT_TOP, type=int,
                        metavar="N",
                        help=f"Also the top N (default {timing_report.DEFAULT_TOP}) functions "
                             "from the profile-*.prof files a --profile run leaves")

    def process(self, args, rest):
        super().process(args, rest)
        plain = getattr(args, "no_ui", False)
        if args.compare:
            before, after = args.compare
            if plain:
                return self._print(timing_report.compare_lines(before, after, args.phase))
            data = timing_report.compare_data(before, after, args.phase)
            from ._timing_view import make_console, render_compare
            render_compare(make_console(), before, after, data)
            return data

        directory = args.dir
        if directory is None:
            cfgs = self.configs or runtime.get_configs(instantiate=False)
            directory = cfgs.log_dir
        if plain:
            return self._print(timing_report.directory_report(directory, args.phase,
                                                              args.profile))
        phases, no_profiles = timing_report.directory_data(directory, args.phase, args.profile)
        from ._timing_view import make_console, render_directory
        render_directory(make_console(), directory, phases, no_profiles)
        return phases

    @staticmethod
    def _print(lines):
        # the report is the command's result
        for line in lines:
            print(line)
        return lines
