"""Artifacts from the Getty vocabularies (docs/artifacts.md).

`ReplacementUris` makes ``getty_replacement_uris.json``: every AAT, ULAN and
TGN id Getty has replaced, with its replacement, as Getty's SPARQL endpoint
returns it (a SPARQL JSON result, ``from`` and ``to``). The reference
manager records a reference to a replaced id under its replacement, and the
Getty fetcher fetches the replacement instead
(`lux_pipeline.process.reference_manager.load_redirects`).
"""

import logging

from .base import ArtifactError, ArtifactGenerator, sparql_json

logger = logging.getLogger("lux_pipeline")

ENDPOINT = "https://vocab.getty.edu/sparql.json"
QUERY = "SELECT ?from ?to WHERE { ?from <http://purl.org/dc/terms/isReplacedBy> ?to }"


class ReplacementUris(ArtifactGenerator):
    """``{"files": ["getty_replacement_uris.json"]}``, with `endpoint` and
    `query` to use other than Getty's and the replaced-ids query."""

    optional_keys = {"endpoint": ENDPOINT, "query": QUERY, "timeout": 600}

    def generate(self, out_dir):
        files = self.config.get("files") or []
        if len(files) != 1:
            raise ArtifactError(f"{self.name}: ReplacementUris writes one file, not {len(files)}")
        endpoint = self.option("endpoint")
        text, bindings = sparql_json(endpoint, self.option("query"), timeout=self.option("timeout"))
        for b in bindings:
            if "from" not in b or "to" not in b:
                raise ArtifactError(f"{endpoint}: a row without from and to: {b}")
        with open(self.out_path(out_dir, files[0]), "w", encoding="utf-8") as fh:
            fh.write(text)
        logger.info(f"{self.name}: {len(bindings):,} replaced ids from {endpoint}")
        return {files[0]: len(bindings)}
