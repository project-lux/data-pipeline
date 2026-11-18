"""The base class of an artifact's generator (docs/artifacts.md).

An artifact is a file, or a few files made together, that a build reads and
that is neither source data nor configuration: the Getty vocabularies'
replaced ids, the language type overrides, the Google Sheet's CSVs. Its
configuration record (``"type": "artifact"``) names a `generatorClass`, a
subclass of `ArtifactGenerator`, and the `files` it writes; the rest of the
record is the generator's own (`required_keys`, `optional_keys`).

A generator reads public services or files, never a source's datacache, so
an artifact does not depend on which sources an installation has. One that
is by nature derived from a source's data names it in `needs_sources`, and
`ArtifactCache.make` refuses to run it without that source.

Generators are built only when an artifact is made (`ArtifactCache.make`),
never by `Config.instantiate_all`, and like every component their
``__init__`` opens nothing.
"""

import json
import logging
import os
import time

import requests

from lux_pipeline.progress import null_progress

logger = logging.getLogger("lux_pipeline")

# what a generator says it is to the services it asks (no contact details)
USER_AGENT = "lux-pipeline (artifact generator)"


class ArtifactError(Exception):
    """An artifact could not be made, published or found."""


class ArtifactGenerator:
    # the record keys the generator cannot work without
    required_keys = ()
    # {key: default} for the ones it reads when they are there
    optional_keys = {}
    # the sources it reads, for an artifact derived from a source's data
    needs_sources = ()

    def __init__(self, config, cfgs, progress=null_progress):
        self.config = config
        self.configs = cfgs
        self.progress = progress
        self.name = config.get("name")

    def option(self, key):
        """The record's value for `key`, else the generator's default."""
        value = self.config.get(key)
        if value is None:
            value = self.optional_keys.get(key)
        return value

    def out_path(self, out_dir, rel):
        """Where file `rel` (one of the record's `files`) is written under
        `out_dir`, its directory made."""
        path = os.path.join(out_dir, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return path

    def generate(self, out_dir):
        """Write every file in ``config["files"]`` under `out_dir` (a
        temporary directory, laid out as ``artifacts_dir`` is) and return
        ``{relative path: row count}``; a count may be None when the format
        has no rows to speak of."""
        raise NotImplementedError


# --- asking SPARQL endpoints --------------------------------------------------------------------


def _sparql_once(endpoint, query, timeout):
    resp = requests.get(endpoint, params={"query": query}, timeout=timeout,
                        headers={"Accept": "application/sparql-results+json",
                                 "User-Agent": USER_AGENT})
    if resp.status_code != 200:
        raise ArtifactError(f"{endpoint} answered {resp.status_code}: {resp.text[:200]}")
    try:
        js = json.loads(resp.text)
        bindings = js["results"]["bindings"]
    except (ValueError, KeyError, TypeError):
        raise ArtifactError(f"{endpoint} did not answer with a SPARQL JSON result: {resp.text[:200]}")
    if not isinstance(bindings, list):
        raise ArtifactError(f"{endpoint}: results.bindings is not a list")
    return resp.text, bindings


def sparql_json(endpoint, query, timeout=600, attempts=3, pause=10):
    """``(text, bindings)``: the SPARQL JSON result of `query` at `endpoint`
    as text, checked to be one (an endpoint in trouble answers with an HTML
    page, a Java stack trace or nothing at all, with a 200), and its
    bindings. Tried `attempts` times, `pause` seconds apart (doubling), on
    a bad answer or a network error; the last one's error is raised."""
    for attempt in range(1, attempts + 1):
        try:
            return _sparql_once(endpoint, query, timeout)
        except (ArtifactError, requests.RequestException) as e:
            if attempt == attempts:
                if isinstance(e, ArtifactError):
                    raise
                raise ArtifactError(f"{endpoint}: {type(e).__name__}: {e}")
            wait = pause * 2 ** (attempt - 1)
            logger.warning(f"{endpoint}: {e}; asking again in {wait}s ({attempt} of {attempts})")
            time.sleep(wait)


# --- counting rows, for the manifest and the shrink check -------------------------------------


def line_rows(path):
    """The lines in a text file (a CSV's rows, header included)."""
    n = 0
    with open(path, "rb") as fh:
        for _ in fh:
            n += 1
    return n


def json_rows(path):
    """The entries of a JSON file: a SPARQL result's bindings, an object's
    keys, a list's items; None for anything else."""
    with open(path, encoding="utf-8") as fh:
        js = json.load(fh)
    if isinstance(js, dict):
        bindings = js.get("results", {}).get("bindings") if isinstance(js.get("results"), dict) else None
        if isinstance(bindings, list):
            return len(bindings)
        return len(js)
    if isinstance(js, list):
        return len(js)
    return None
