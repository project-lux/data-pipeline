import json
import os
import sys

from dotenv import load_dotenv

from pipeline.config import Config

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

yuag = cfgs.internal['yuag']['recordcache']

for hmo in yuag.iter_records_type('HumanMadeObject'):
    identifiers = hmo['data']['identified_by']
    acts = hmo['data'].get('used_for', [])
    if acts:
        print(acts)
        print(identifiers)

