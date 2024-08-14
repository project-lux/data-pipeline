import os
import sys
import json
import logging
import requests
from dotenv import load_dotenv
from pipeline.config import Config
from requests.exceptions import RequestException

def setup_logging():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def load_configuration() -> Config:
    load_dotenv()
    basepath = os.getenv("LUX_BASEPATH", "")
    cfgs = Config(basepath=basepath)
    cfgs.debug_reconciliation = True  # Ensure debug is on
    cfgs.cache_globals()
    cfgs.instantiate_all()
    return cfgs

def fetch_record(uri: str) -> dict:
    try:
        response = requests.get(uri)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        return response.json()
    except RequestException as e:
        logging.error(f"Could not fetch URI {uri}: {e}")
        return {}

def process_equivalents(cfgs: Config, equivalents: list) -> dict:
    recnames = {}
    for e in equivalents:
        ident = e.get("id", "")
        if ident:
            src, identifier = cfgs.split_uri(ident)
            cache = src.get('recordcache', {})
            if cache:
                primary_name = cache.get(identifier, {}).get('primary_name')
                if primary_name:
                    recnames[ident] = primary_name
                logging.debug(f"Processed equivalent {ident}: {primary_name}")
    return recnames

def main(uri: str):
    setup_logging()
    cfgs = load_configuration()
    
    record = fetch_record(uri)
    if not record:
        return

    record_type = uri.rsplit("/", 2)[-2]
    if record_type == "concept":
        record_type = "type"
    record_type = record_type.title()
    
    equivalents = record.get("equivalent", [])
    if equivalents:
        recnames = process_equivalents(cfgs, equivalents)
        logging.info(f"Primary names found: {recnames}")
    else:
        logging.warning(f"No equivalents found for {uri}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python kellys-debugger.py <URI>")
        sys.exit(1)
    uri_input = sys.argv[1]
    main(uri_input)
