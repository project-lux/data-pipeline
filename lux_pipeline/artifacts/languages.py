"""The languages artifact: ``type_overrides.json`` (docs/artifacts.md).

`LanguageTypes` makes ``{uri: "Language"}`` for every identifier of a
language that a record may give as an equivalent: the Wikidata items with an
ISO 639 code, and their AAT, LCSH and LC vocabulary identifiers. Every
mapper reads it and, as it maps a record (`Mapper.apply_type_overrides`, in
`post_mapping`), gives a ``Type`` record that is one of them or has one as
an equivalent the type ``Language``.

It asks two SPARQL endpoints and needs no source, so an installation without
Wikidata as a source still has it:

* Wikidata: the items with any of `codeProperties` (default ``P218``,
  ``P219``, ``P220``: ISO 639-1, -2 and -3), then, for those items, their
  ``P1014`` (AAT), ``P244`` (LC; only subject headings, ``sh...``, are
  kept, a language being a subject) and ``P4801`` (LC vocabularies; only
  those under `lcVocabularies`, default ``languages``).
* Getty: every concept under `aatRoot` (default ``300389739``, "languages
  and writing systems"), and the replaced ids whose replacement is one. An
  AAT id from Wikidata is kept only when it is one of those: Wikidata
  editors put the AAT concept of a people or a culture on many language
  items. A replaced id is kept along with its replacement.
"""

import json
import logging

from .base import ArtifactError, ArtifactGenerator, sparql_json

logger = logging.getLogger("lux_pipeline")

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
GETTY_ENDPOINT = "https://vocab.getty.edu/sparql.json"
WD = "http://www.wikidata.org/entity/"
AAT = "http://vocab.getty.edu/aat/"
LCSH = "http://id.loc.gov/authorities/subjects/"
LC_VOCABULARY = "http://id.loc.gov/vocabulary/"
# Wikidata's identifier properties, by what they identify
AAT_PROP, LC_PROP, LC_VOCAB_PROP = "P1014", "P244", "P4801"


def code_path(props):
    """The code properties as a property path: Wikidata's Blazegraph does
    not narrow ``?item ?code []`` by a VALUES list of them, and answers
    with every triple it has until it times out."""
    return "|".join(f"wdt:{p}" for p in props)


def items_query(props):
    return f"SELECT DISTINCT ?item WHERE {{ ?item {code_path(props)} [] }}"


def identifier_query(props, prop):
    return f"SELECT DISTINCT ?item ?value WHERE {{ ?item {code_path(props)} [] ; wdt:{prop} ?value }}"


def aat_concepts_query(root):
    return f"SELECT ?s WHERE {{ ?s gvp:broaderExtended aat:{root} }}"


def aat_replaced_query(root):
    return ("SELECT ?old ?new WHERE { ?old <http://purl.org/dc/terms/isReplacedBy> ?new . "
            f"?new gvp:broaderExtended aat:{root} }}")


class LanguageTypes(ArtifactGenerator):
    """``{"files": ["type_overrides.json"]}``; every other key optional."""

    optional_keys = {
        "wikidataEndpoint": WIKIDATA_ENDPOINT,
        "gettyEndpoint": GETTY_ENDPOINT,
        "codeProperties": ["P218", "P219", "P220"],
        "aatRoot": "300389739",
        "lcVocabularies": ["languages"],
        "timeout": 300,
    }

    def ask(self, endpoint, query):
        return sparql_json(endpoint, query, timeout=self.option("timeout"))[1]

    def language_items(self):
        """The Wikidata items with an ISO 639 code: ``{item URI}``."""
        rows = self.ask(self.option("wikidataEndpoint"), items_query(self.option("codeProperties")))
        items = {r["item"]["value"] for r in rows if "item" in r}
        if not items:
            raise ArtifactError(f"{self.option('wikidataEndpoint')}: no item has any of "
                                f"{', '.join(self.option('codeProperties'))}")
        return items

    def identifiers(self, prop):
        """``[(item URI, value)]`` of property `prop` on the language items."""
        rows = self.ask(self.option("wikidataEndpoint"),
                        identifier_query(self.option("codeProperties"), prop))
        return [(r["item"]["value"], r["value"]["value"]) for r in rows
                if "item" in r and "value" in r]

    def aat_languages(self):
        """``(concepts, replaced)``: every AAT URI under `aatRoot`, and
        ``{replaced URI: its replacement}`` for the replaced ones whose
        replacement is one of them."""
        endpoint, root = self.option("gettyEndpoint"), self.option("aatRoot")
        concepts = {r["s"]["value"] for r in self.ask(endpoint, aat_concepts_query(root)) if "s" in r}
        if not concepts:
            raise ArtifactError(f"{endpoint}: nothing under aat:{root}")
        replaced = {r["old"]["value"]: r["new"]["value"]
                    for r in self.ask(endpoint, aat_replaced_query(root)) if "old" in r and "new" in r}
        return concepts, replaced

    def overrides(self):
        """``({uri: "Language"}, counts)``: what the file holds, and how many
        of each kind were kept or dropped, for the log."""
        items = self.language_items()
        concepts, replaced = self.aat_languages()
        vocabularies = tuple(f"{v}/" for v in self.option("lcVocabularies"))
        keep = set(items)
        counts = {"wikidata": len(items), "aat": 0, "aat replaced": 0, "aat not a language": 0,
                  "lcsh": 0, "lc not a subject": 0, "lc vocabulary": 0, "lc vocabulary other": 0}
        for item, value in self.identifiers(AAT_PROP):
            uri = AAT + value.strip()
            if uri in concepts:
                keep.add(uri)
                counts["aat"] += 1
            elif uri in replaced:
                keep.update((uri, replaced[uri]))
                counts["aat replaced"] += 1
            else:
                counts["aat not a language"] += 1
                logger.debug(f"{self.name}: {uri} (on {item}) is not under aat:{self.option('aatRoot')}")
        for _, value in self.identifiers(LC_PROP):
            if value.startswith("sh"):
                keep.add(LCSH + value.strip())
                counts["lcsh"] += 1
            else:
                counts["lc not a subject"] += 1
        for _, value in self.identifiers(LC_VOCAB_PROP):
            if value.startswith(vocabularies):
                keep.add(LC_VOCABULARY + value.strip())
                counts["lc vocabulary"] += 1
            else:
                counts["lc vocabulary other"] += 1
        return {uri: "Language" for uri in sorted(keep)}, counts

    def generate(self, out_dir):
        files = self.config.get("files") or []
        if len(files) != 1:
            raise ArtifactError(f"{self.name}: LanguageTypes writes one file, not {len(files)}")
        data, counts = self.overrides()
        with open(self.out_path(out_dir, files[0]), "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=0, ensure_ascii=False)
            fh.write("\n")
        logger.info(f"{self.name}: {len(data):,} identifiers of languages: "
                    + ", ".join(f"{k} {v:,}" for k, v in counts.items()))
        return {files[0]: len(data)}
