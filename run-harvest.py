import os
import sys
import json
import time
from dotenv import load_dotenv
from pipeline.config import Config
from pipeline.process.update_manager import UpdateManager

load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
idmap = cfgs.instantiate_map('idmap')['store']
cfgs.cache_globals()
cfgs.instantiate_all()


### FIXME: Process command line in the same way as other 
# scripts, e.g. --ypm --yuag --all (etc)

ypm = cfgs.internal['ypm']
#last_update = "2024-01-22T19:17:17"
#ypm['harvester'].last_update = last_update

mgr = UpdateManager(cfgs, idmap)
mgr.harvest_single('ypm')
