url=$(python scripts/download/viaf.py)
echo "Downloading VIAF data from $url"
curl -O "$url"