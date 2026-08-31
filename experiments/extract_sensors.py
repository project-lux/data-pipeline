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
        if type(acts) is dict:
            acts = [acts]
        sids = []
        acc_no = ""
        sys_no = ""
        for act in acts:
            cxns = [x['id'] for x in act.get('classified_as', [])]
            if "http://vocab.getty.edu/aat/300379380" in cxns:
                # found sensors
                sensors = act.get('used_specific_object', [])
                sids = []
                for sensor in sensors:
                    if 'identified_by' in sensor:
                        sids.append(sensor['identified_by'][0]['content'])

        if sids:
            for ident in identifiers:
                if ident['type'] == 'Identifier':   
                    cxns = [x['id'] for x in ident.get('classified_as', [])]                
                    if "http://vocab.getty.edu/aat/300312355" in cxns:
                        acc_no = ident['content']
                    elif "http://vocab.getty.edu/aat/300435704" in cxns:
                        sys_no = ident['content']
            print(f"{acc_no}|{sys_no}|{';'.join(sids)}")

