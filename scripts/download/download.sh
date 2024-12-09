#!/bin/bash
# This script downloads data files from various sources.
#
# For each source, it uses a Python script to fetch and parse the source's data page
# to get the URL of the latest data file. Then it downloads that file using curl.
#
# Currently supported sources:
# - viaf: Downloads VIAF clusters data file (viaf-NNNNNNNN-clusters.xml.gz)
#
# The Python scripts will exit with status 1 if they fail to fetch the URL or if no
# valid file URL is found. Otherwise they print just the URL to stdout.

# Example usage:
# sudo bash scripts/download.sh --source=viaf

source=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --source=*)
            source="${1#*=}"
            shift
            ;;
        *)
            echo "Usage: $0 --source=<source>"
            exit 1
            ;;
    esac
done

if [ -z "$source" ]; then
    echo "Usage: $0 --source=<source>"
    exit 1
fi

case "$source" in
    "viaf"|"dnb")
        urls=$(python scripts/download/sources/$source.py)
        if [ $? -ne 0 ]; then
            echo "Error: Failed to fetch URLs for '$source'"
            exit 1
        fi
        
        # Parse JSON array using jq for both sources
        echo "$urls" | jq -r '.[]' | while read -r url; do
            if [ ! -z "$url" ]; then
                echo "Downloading '$source' data from $url"
                curl -O "$url"
                if [ $? -ne 0 ]; then
                    echo "Error: Failed to download from $url"
                    exit 1
                fi
            fi
        done
        ;;
    *)
        echo "Error: Unknown source '$source'"
        echo "Supported sources: viaf, dnb"
        exit 1
        ;;
esac