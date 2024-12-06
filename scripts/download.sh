#!/bin/bash

# This script downloads the latest VIAF data file.
#
# It first uses a Python script to fetch and parse the VIAF data page to get the URL
# of the latest data file (in the format viaf-NNNNNNNN-clusters.xml.gz).
# Then it downloads that file using curl.
#
# The Python script will exit with status 1 if it fails to fetch the URL or if no
# valid file URL is found. Otherwise it prints just the URL to stdout.

# Example usage:
# sudo bash scripts/download.sh  --source=viaf

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
    "viaf")
        url=$(python scripts/sources/$source.py)
        echo "Downloading '$source' data from $url"
        curl -O "$url"
        ;;
    *)
        echo "Error: Unknown source '$source'"
        echo "Supported sources: viaf"
        exit 1
        ;;
esac