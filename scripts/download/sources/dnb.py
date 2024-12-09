"""
This script is called by download.sh to fetch a list of URLs.

The URLs point to open data files in json-ld format from the Deutsche Nationalbibliothek (DNB).

The three files are (in order):
   - GND-Sachbegriff: Subject Headings (without sameAs relationships)
   - GND-EntityFacts: Person, Family, Corporate Body, Geographical Entity, Event 
     (with sameAs relationships)
   - GND-LCSH-Rameau: GND <==> LCSH <==> BNF equivalent relationships for both Sachbegriff and EntityFacts 
     data, created during Multilingual Access to Subject (MACS) project, 1997-2016

Usage:
    Example usage in download.sh:
        urls=$(python scripts/download/sources/dnb.py)

Output:
    - Each URL is printed on a new line to standard output, which is captured by download.sh.

Error Handling:
    - If an exception occurs, an error message is printed to standard error, and
      the script exits with status code 1.
"""


import sys

urls = ["https://data.dnb.de/opendata/authorities-gnd-sachbegriff_lds.jsonld.gz", 
        "https://data.dnb.de/opendata/authorities-gnd_entityfacts.jsonld.gz",
        "https://data.dnb.de/opendata/mapping-authorities-gnd-lcsh-ram_lds.jsonld.gz"]


if __name__ == "__main__":
    try:
        for url in urls:
            print(url)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
