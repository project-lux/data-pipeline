#!/usr/bin/env python3
"""Convert legacy assertion logs to the new AssertionWriter format.

The old reconcile phase wrote two-column, full-URI lines::

    <asserter/subject qua-uri>\t<equivalent qua-uri>

(the asserter was implicitly the first column, and a record with no
equivalents wrote ``uri\turi``).

The new AssertionWriter (pipeline/process/identity.py) writes three columns
of *shortened* (curie) URIs in canonical order::

    <lesser-uri>\t<greater-uri>\t<asserter-uri>

so the files can be sorted and streamed without loading them into memory.

This utility rewrites an existing directory of legacy files into the new
format, using the same namespace map the idmap/AssertionWriter use. It is a
one-time migration for assertion logs produced before the format change;
new runs of run-reconcile.py already emit the new format directly.

Usage:
    python convert_assertions.py INPUT_DIR OUTPUT_DIR

The namespace map comes from the real pipeline Config when it can be loaded
(so the short forms match redis exactly); otherwise a self-contained
fallback map covering the LUX source namespaces is used (fine for testing /
standalone conversion, where nothing needs to line up with a live idmap).
"""

import glob
import os
import sys
import time

from pipeline.process.identity import build_prefix_maps, expand, shorten


# ---------------------------------------------------------------------------
# namespace map
# ---------------------------------------------------------------------------

class _FallbackConfig:
    """Enough of Config for build_prefix_maps() when the real config_cache
    is not available. Covers the LUX internal data sources and external
    authorities. Short names are collision-free (no name is a prefix of
    another) so expand() round-trips."""

    internal_uri = "https://lux.collections.yale.edu/data/"

    # name -> namespace prefix. Order does not matter for shorten() because
    # no namespace is a prefix of another distinct namespace.
    _NS = [
        # LUX data sources
        ("lald", "https://linked-art.library.yale.edu/"),
        ("ypm", "https://images.peabody.yale.edu/"),
        ("yuag", "https://media.art.yale.edu/"),
        ("ycba", "https://ycba-lux.s3.amazonaws.com/v3/"),
        ("pmc", "https://data.paul-mellon-centre.ac.uk/"),
        ("pb", "https://paperbase.xyz/"),
        ("exm", "http://lod.example.org/museum/"),
        # external authorities
        ("aat", "http://vocab.getty.edu/aat/"),
        ("ulan", "http://vocab.getty.edu/ulan/"),
        ("tgn", "http://vocab.getty.edu/tgn/"),
        ("wikidata", "http://www.wikidata.org/entity/"),
        ("wikimedia", "http://commons.wikimedia.org/"),
        ("viaf", "http://viaf.org/viaf/"),
        ("geonames", "https://sws.geonames.org/"),
        ("wof", "http://data.whosonfirst.org/"),
        ("dnb", "https://d-nb.info/gnd/"),
        ("bnf", "https://data.bnf.fr/ark:/12148/"),
        ("bne", "https://datos.bne.es/resource/"),
        ("gbif", "https://www.gbif.org/species/"),
        ("lcnaf", "http://id.loc.gov/authorities/names/"),
        ("lcsh", "http://id.loc.gov/authorities/subjects/"),
        ("lcdgt", "http://id.loc.gov/authorities/demographicTerms/"),
        ("ror", "https://ror.org/"),
    ]

    def __init__(self):
        self.external = {name: {"name": name, "namespace": ns}
                         for name, ns in self._NS}


def load_prefix_maps():
    """(prefix_in, prefix_out, source-label). Prefer the real Config so the
    short forms match the live idmap; fall back to the built-in LUX map."""
    try:
        from pipeline.config import Config
        cfgs = Config(basepath=os.getenv("LUX_BASEPATH", ""))
        pin, pout = build_prefix_maps(cfgs)
        return pin, pout, "pipeline.config.Config"
    except Exception as e:
        pin, pout = build_prefix_maps(_FallbackConfig())
        return pin, pout, f"fallback map ({type(e).__name__} loading Config)"


# ---------------------------------------------------------------------------
# conversion
# ---------------------------------------------------------------------------

def convert_file(src, dst, prefix_in):
    """Stream one legacy file into the new format. Returns (lines, self)."""
    lines = 0
    selfs = 0
    with open(src) as fin, open(dst, "w") as fout:
        for line in fin:
            parts = line.rstrip("\n").split("\t")
            if len(parts) == 3:
                # already new format: pass through unchanged
                fout.write(line if line.endswith("\n") else line + "\n")
                lines += 1
                continue
            if len(parts) != 2:
                continue
            a, b = parts
            asserter = shorten(a, prefix_in)
            sa = asserter
            sb = shorten(b, prefix_in)
            lo, hi = (sa, sb) if sa <= sb else (sb, sa)
            fout.write(f"{lo}\t{hi}\t{asserter}\n")
            lines += 1
            if a == b:
                selfs += 1
    return lines, selfs


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(2)
    in_dir, out_dir = sys.argv[1], sys.argv[2]
    os.makedirs(out_dir, exist_ok=True)

    prefix_in, prefix_out, source = load_prefix_maps()
    print(f"namespace map: {source} ({len(prefix_out)} prefixes)")

    files = sorted(glob.glob(os.path.join(in_dir, "assertions-*.tsv")))
    if not files:
        print(f"No assertions-*.tsv in {in_dir}")
        sys.exit(1)

    total_lines = total_self = 0
    in_bytes = out_bytes = 0
    t0 = time.time()
    for src in files:
        dst = os.path.join(out_dir, os.path.basename(src))
        lines, selfs = convert_file(src, dst, prefix_in)
        total_lines += lines
        total_self += selfs
        in_bytes += os.path.getsize(src)
        out_bytes += os.path.getsize(dst)
        print(f"  {os.path.basename(src):24s} {lines:>10,} lines "
              f"({selfs:,} self)")
    dt = time.time() - t0

    print(f"\n{len(files)} files, {total_lines:,} lines "
          f"({total_self:,} self-assertions) in {dt:.0f}s")
    print(f"size: {in_bytes/1e9:.2f} GB -> {out_bytes/1e9:.2f} GB "
          f"({100 * out_bytes / in_bytes:.0f}% of original)")


if __name__ == "__main__":
    main()
