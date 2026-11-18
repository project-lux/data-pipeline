"""Artifacts from the Getty vocabularies (docs/artifacts.md).

`ReplacementUris` makes ``getty_replacement_uris.json``: every AAT, ULAN and
TGN id Getty has replaced, with its replacement, as Getty's SPARQL endpoint
returns it (a SPARQL JSON result, ``from`` and ``to``). The reference
manager records a reference to a replaced id under its replacement, and the
Getty fetcher fetches the replacement instead
(`lux_pipeline.process.reference_manager.load_redirects`).
"""

import json
import logging

import requests

from .base import ArtifactError, ArtifactGenerator

logger = logging.getLogger("lux_pipeline")

ENDPOINT = "https://vocab.getty.edu/sparql.json"
QUERY = "SELECT ?from ?to WHERE { ?from <http://purl.org/dc/terms/isReplacedBy> ?to }"
USER_AGENT = "lux-pipeline (artifact generator)"


def sparql_json(endpoint, query, timeout=600):
    """The SPARQL JSON result of `query` at `endpoint`, as text, checked to
    be one (an endpoint in trouble answers with an HTML page or a Java
    stack trace and a 200)."""
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
