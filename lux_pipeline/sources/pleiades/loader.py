"""Pleiades: two files, both ``input_files`` records entries.

* ``pleiades-places-latest.json.gz``, a JSON-LD document whose places
  are the items of ``@graph``: ``graph.gz/raw``. The key is the place's
  ``id`` (``48210385``), else the last segment of its ``uri``.
* ``place-types.ttl``, the place-type vocabulary in Turtle: the ``ttl``
  container yields each concept's block, from its
  ``<https://pleiades.stoa.org/vocabularies/...>`` subject line to the
  closing ``.``; the ``other`` format stores it as ``{"data": <block>}``
  under the concept id (``abbey``), the text the mapper's
  ``parse_types`` reads.
"""

import io
import re

from lux_pipeline.process.base.loader import Loader

CONCEPT = re.compile(r"^<https://pleiades\.stoa\.org/vocabularies/([^>]+)>")


class PleiadesLoader(Loader):
    extra_containers = ("ttl",)

    def _init_dsl(self):
        super()._init_dsl()
        self.step_functions["ttl"] = self.iterate_ttl

    def iterate_ttl(self, path, comp, remaining):
        """Concept blocks of the Turtle file; comments and prefixes are
        skipped. The whole file is read (it is small)."""
        with self.file_opener(path, comp) as fh:
            text = fh.read()
        if isinstance(text, bytes):
            text = text.decode("utf-8")
        blocks, current = [], []
        for line in text.splitlines():
            if CONCEPT.match(line):
                if current:
                    blocks.append("\n".join(current))
                current = [line]
            elif current and line.strip():
                current.append(line)
            if current and line.rstrip().endswith(" ."):
                blocks.append("\n".join(current))
                current = []
        if current:
            blocks.append("\n".join(current))
        if len(remaining) == 1:
            self._add_total(len(blocks))
        for b in blocks:
            yield io.BytesIO(b.encode("utf-8"))

    def extract_identifier(self, data):
        if not isinstance(data, dict):
            return None
        if data.get("id"):
            return self.make_identifier(str(data["id"]))
        if data.get("uri"):
            return self.make_identifier(data["uri"])
        text = data.get("data")
        if isinstance(text, str):
            m = CONCEPT.match(text)
            if m:
                return m.group(1)
        return None
