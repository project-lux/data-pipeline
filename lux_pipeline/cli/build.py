"""Run a whole build: sheets, reconcile, merge, portal, export.

    lux build                                  # run-all.sh --all, phase by phase
    lux build --clear                          # clear the caches and refs, new token, then build
    lux build --max-workers 24 --no-ui         # global options go to every phase
    lux build --from merge                     # pick up again at a phase
    lux build --skip sheets,portal             # leave phases out
    lux build --dry-run                        # print the steps, run nothing

`run-all.sh` (and `run-all2.sh` with ``ENGINE=mp``) as one command. The
phases, in order:

    clear      only with --clear: lux clear --source all --force,
               lux clear --refs --force, lux new-token
    sheets     lux sheets: read the curators' spreadsheet, load its same/different pairs
    reconcile  lux reconcile --source <S> (and so identify, metatypes, write-refs)
    merge      lux merge --source <S>
    portal     lux portal: tag the records the YPM portal needs
    export     lux export

Each step is a separate ``lux`` process, so each phase loads a fresh
configuration and gets its own status/log prefix for ``lux watch``;
``--prefix P`` gives them ``P-<phase>``. Each phase's command removes what
an earlier build left behind (metatypes slice files, done flags, old
export slices) before its workers start. The first step to fail stops the
build. Run it from the repository, as the shell script: the
``metatypes-*.json`` files and ``reference_uris.txt`` are written in the
working directory. Success and failure are appended, as by run-all.sh, to
``--status-file`` (default ``<log_dir>/pipeline_process_status.txt``).
"""

import logging
import os
import shlex
import subprocess
import sys
from datetime import datetime

from lux_pipeline import runtime

from ._handler import BaseHandler, refuse_while_building

logger = logging.getLogger("lux_pipeline")

PHASES = ("clear", "sheets", "reconcile", "merge", "portal", "export")


class CommandHandler(BaseHandler):
    # the phases load their own configuration; this process only needs the
    # directories, and must not hold the idmap open while identify rewrites it
    needs_configs = False

    def add_args(self, ap):
        ap.add_argument("--clear", action="store_true",
                        help="First clear every source's computed caches and the reference "
                             "queues, and mint a new update token")
        ap.add_argument("--from", dest="start", choices=PHASES,
                        help="Start at this phase")
        ap.add_argument("--to", dest="stop", choices=PHASES,
                        help="Stop after this phase")
        ap.add_argument("--skip", default="",
                        help=f"Phases to leave out, separated by commas: {', '.join(PHASES)}")
        ap.add_argument("--profile", action="store_true",
                        help="Pass --profile to reconcile, merge and export")
        ap.add_argument("--status-file", dest="status_file",
                        help="Append the outcome here "
                             "(default <log_dir>/pipeline_process_status.txt)")
        ap.add_argument("--dry-run", "--dry_run", dest="dry_run", action="store_true",
                        help="Print the steps and run nothing")
        ap.add_argument("--force", action="store_true",
                        help="Run even though a build looks to be running")

    # --- planning ----------------------------------------------------------

    def phases(self, args):
        """The phases to run, in order, from --clear, --from, --to and --skip."""
        skip = [s.strip() for s in (args.skip or "").split(",") if s.strip()]
        for s in skip:
            if s not in PHASES:
                raise ValueError(f"Unknown phase in --skip: {s} (phases: {', '.join(PHASES)})")
        start = PHASES.index(args.start) if args.start else 0
        stop = PHASES.index(args.stop) if args.stop else len(PHASES) - 1
        if start > stop:
            raise ValueError(f"--from {args.start} comes after --to {args.stop}")
        out = []
        for p in PHASES[start:stop + 1]:
            if p in skip or (p == "clear" and not args.clear):
                continue
            out.append(p)
        if args.clear and "clear" not in out:
            raise ValueError("--clear, but the clear phase is outside --from / --to / --skip")
        return out

    def lux(self, args, command, *opts, phase=None, workers=True):
        """argv for one ``lux`` step, with the global options passed on:
        all of them to a phase run by workers, only the log level to a
        one-shot step, and to ``portal`` (its own process pool) the
        worker count too."""
        argv = [sys.executable, "-m", "lux_pipeline.cli.entry", command, *opts]
        if args.log:
            argv += ["--log", args.log]
        if args.debug:
            argv.append("--debug")
        if workers == "count":
            if args.max_workers > 0:
                argv += ["--max-workers", str(args.max_workers)]
        elif workers:
            if args.max_workers > 0:
                argv += ["--max-workers", str(args.max_workers)]
            if args.max_records > -1:
                argv += ["--max-records", str(args.max_records)]
            if args.engine:
                argv += ["--engine", args.engine]
            if args.no_ui:
                argv.append("--no-ui")
            if args.prefix:
                argv += ["--prefix", f"{args.prefix}-{phase or command}"]
            if args.profile:
                argv.append("--profile")
        return argv

    def steps(self, args):
        """``[(phase, argv)]`` for the whole run."""
        source = args.source or "all"
        out = []
        for phase in self.phases(args):
            if phase == "clear":
                out += [
                    (phase, self.lux(args, "clear", "--source", "all", "--force", workers=False)),
                    (phase, self.lux(args, "clear", "--refs", "--force", workers=False)),
                    (phase, self.lux(args, "new-token", workers=False)),
                ]
            elif phase == "sheets":
                out.append((phase, self.lux(args, "sheets", workers=False)))
            elif phase in ("reconcile", "merge"):
                out.append((phase, self.lux(args, phase, "--source", source)))
            elif phase == "portal":
                out.append((phase, self.lux(args, "portal", workers="count")))
            elif phase == "export":
                out.append((phase, self.lux(args, "export")))
        return out

    # --- running -----------------------------------------------------------

    def run_step(self, argv):
        """Run one step in the foreground; its exit status."""
        return subprocess.run(argv).returncode

    def record(self, path, line):
        if not path:
            return
        try:
            with open(path, "a") as fh:
                fh.write(f"{datetime.now().strftime('%a %b %d %H:%M:%S %Y')} {line}\n")
        except OSError as e:
            logger.warning(f"could not write the status file {path}: {e}")

    def process(self, args, rest):
        super().process(args, rest)
        if args.my_worker > -1:
            raise ValueError("lux build runs every slice itself; --my-worker does not apply")
        cfgs = self.configs or runtime.get_configs(instantiate=False)
        if args.source and args.source not in ("all", "internal"):
            # the phases check the names; catch a typo before clearing anything
            known = set(cfgs.internal) | set(cfgs.external)
            for n in (s.strip() for s in args.source.split(",")):
                if n and n not in known:
                    raise ValueError(f"Unknown source: {n}")
        steps = self.steps(args)
        status_file = args.status_file
        if status_file is None and getattr(cfgs, "log_dir", None):
            status_file = os.path.join(cfgs.log_dir, "pipeline_process_status.txt")

        if args.dry_run:
            for phase, step in steps:
                print(f"[{phase}] {shlex.join(step)}")
            return steps

        refuse_while_building(cfgs, "build", args.force)
        current = None
        for phase, step in steps:
            if phase != current:
                current = phase
                print(f"=== {phase} ===", flush=True)
            logger.info(f"running: {shlex.join(step)}")
            code = self.run_step(step)
            if code != 0:
                self.record(status_file, f"[Error] Error in {phase}")
                raise RuntimeError(f"build stopped in {phase}: {shlex.join(step)} "
                                   f"exited with {code}")
        self.record(status_file, "[Success] Build was successful")
        print("Build finished")
        return steps
