"""`RefetchManager`: fetch records again from their source and store the
fresh copies in the datacache. The manager behind `lux refetch`; replaces
``manage-data.py --reload`` and updateExternal.py.

Both of those set ``acquirer.force_rebuild``, which nothing has read since
the acquirer moved to a ``refetch`` argument, and so returned whatever the
datacache already held: they re-fetched nothing that was cached. This calls
``acquirer.do_fetch(identifier, store=True, refetch=True)``, which goes to
the network and stores what comes back (under the identifier the source
redirects to, if it does). Only data is fetched: nothing is mapped, and
the record caches are left as they are.

Which identifiers:

* by default the records a build *used*: the keys of each source's
  recordcache, ``##qua<type>`` removed (updateExternal.py, and ``--reload
  --wikidata``). Each worker takes a slice of the recordcache and fetches
  each identifier in it once; an identifier cached under two types can fall
  in two slices and be fetched twice.
* ``all_records`` (``--all-records``): every key of the datacache
  (``--reload --tgn`` / ``--ulan``).

The worker turns on the fetcher of each source it re-fetches (the
configuration's ``allow_network`` is usually false, which leaves fetchers
disabled). A source in ``ignore_sources`` or without a fetcher is skipped.
Per source a worker counts ``fetched``, ``empty`` (the source returned
nothing: gone, or not found), ``redirected`` (stored under another
identifier) and ``failed`` (an exception, logged and carried past).
"""

import logging

from ._task_manager import TaskManager

logger = logging.getLogger("lux_pipeline")

# failures logged in full per worker and source; the rest are only counted
LOG_FAILURES = 20


class RefetchManager(TaskManager):
    """Option, read from `args` in ``__init__``: ``all_records``
    (``--all-records``: every datacache key, not just the ones the
    recordcache used)."""

    def __init__(self, configs, max_workers=0, args=None):
        super().__init__(configs, max_workers, args)
        self.all_records = False
        self.results = None
        if args is not None:
            self.all_records = bool(getattr(args, "all_records", False))

    # ----------------------------------------------------------- sources

    def maybe_add(self, which, cfg):
        """Sources that can fetch: an acquirer and a fetcher, not ignored."""
        name = cfg["name"]
        acq, fetcher = cfg.get("acquirer"), cfg.get("fetcher")
        if acq is None or fetcher is None:
            logger.warning(f"{name}: no acquirer or fetcher, nothing to refetch with; skipped")
            return
        if name in (getattr(acq, "ignore_sources", None) or []):
            logger.warning(f"{name}: in ignore_sources, so its acquirer never fetches; skipped")
            return
        if (which, name) not in self.sources:
            self.sources.append((which, name))

    def check(self):
        if not self.sources:
            raise ValueError("No source to refetch: name sources that have a fetcher")

    # ----------------------------------------------------------- driver

    def process(self, engine="mp", prefix=None, view=None):
        self.check()
        self.results = super().process(engine, prefix, view)
        return self.results

    def summary(self):
        """``{source: counts}`` added up over the workers."""
        out = {}
        for r in self.results or []:
            if not isinstance(r, dict):
                continue
            for src, counts in r.items():
                tot = out.setdefault(src, dict.fromkeys(counts, 0))
                for k, v in counts.items():
                    tot[k] = tot.get(k, 0) + v
        return out

    # ----------------------------------------------------------- workers

    def identifiers(self, cfgs, cfg, n):
        """This worker's identifiers for one source, each once."""
        mx = self.max_workers
        cache = cfg["datacache"] if self.all_records else cfg["recordcache"]
        keys = cache.iter_keys_slice(n, mx) if mx > 1 else cache.iter_keys()
        if self.all_records:
            yield from keys
            return
        seen = set()
        for key in keys:
            ident = cfgs.split_qua(key)[0]
            if ident not in seen:
                seen.add(ident)
                yield ident

    def _distributed(self, n, cfgs, progress):
        out = {}
        mx = self.max_workers
        for which, name in self.sources:
            cfg = getattr(cfgs, which)[name]
            acq = cfg["acquirer"]
            cfg["fetcher"].enabled = True
            counts = {"fetched": 0, "empty": 0, "redirected": 0, "failed": 0}
            out[name] = counts
            cache = cfg["datacache"] if self.all_records else cfg["recordcache"]
            est = getattr(cache, "len_estimate", None)
            progress(phase="refetch", description=name,
                     total=(est() if est else 0) // max(mx, 1), completed=0)
            done = 0
            for ident in self.identifiers(cfgs, cfg, n):
                if self.max_records > -1 and done >= self.max_records:
                    break
                done += 1
                progress(advance=1)
                try:
                    rec = acq.do_fetch(ident, store=True, refetch=True)
                except Exception as e:
                    counts["failed"] += 1
                    if counts["failed"] <= LOG_FAILURES:
                        logger.warning(f"{name}/{ident}: {type(e).__name__}: {e}")
                    continue
                if rec is None:
                    counts["empty"] += 1
                    continue
                counts["fetched"] += 1
                got = rec.get("identifier") if isinstance(rec, dict) else None
                if got is not None and got != ident:
                    counts["redirected"] += 1
            logger.info(f"{name}: {counts['fetched']:,} fetched, {counts['empty']:,} empty, "
                        f"{counts['redirected']:,} redirected, {counts['failed']:,} failed")
        return out
