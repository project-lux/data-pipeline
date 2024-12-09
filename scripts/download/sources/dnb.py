"""
This script is called by download.sh to fetch a list of URLs.

The three URLs point to files from the Deutsche Nationalbibliothek (DNB) in json-ld gzip format.

The files are (in order):
   - GND-Sachbegriff: Subject Headings (without sameAs relationships)
   - GND-EntityFacts: Person, Family, Corporate Body, Geographical Entity, Event 
     (with sameAs relationships)
   - GND-LCSH-Rameau: GND <==> LCSH <==> BNF equivalent relationships for both Sachbegriff and EntityFacts 
     data, created during Multilingual Access to Subject (MACS) project, 1997-2016

Usage:
    Example usage in download.sh:
        urls=$(python scripts/download/sources/dnb.py)

Output:
    - Returns the list of URLs to be used by the calling script.

Error Handling:
    - If an exception occurs, an error message is printed to standard error, and
      the script exits with status code 1.
"""


import sys
import json

def main():
    urls = ["https://data.dnb.de/opendata/authorities-gnd-sachbegriff_lds.jsonld.gz", 
            "https://data.dnb.de/opendata/authorities-gnd_entityfacts.jsonld.gz",
            "https://data.dnb.de/opendata/mapping-authorities-gnd-lcsh-ram_lds.jsonld.gz"
    ]
    print(json.dumps(urls))
    return urls

if __name__ == "__main__":
    try:
        urls = main()
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
