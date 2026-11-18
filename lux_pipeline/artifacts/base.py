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
import os

from lux_pipeline.progress import null_progress


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
