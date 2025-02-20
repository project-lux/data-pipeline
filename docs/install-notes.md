# Installation notes


## Running on Mac: 
* Install postgres: I use https://postgresapp.com/ 
* Install pgadmin: I use https://www.pgadmin.org/download/pgadmin-4-macos/
* Install homebrew: I use the .pkg installer linked from https://brew.sh/ 
* Update homebrew: brew update
* Install redis:  brew install redis
* Set both postgres and redis to start at login

## Code

* Create home directory for LUX
* Clone the repository: `git clone https://github.com/project-lux/data-pipeline.git`
* Create dot env file:  `echo "LUX_BASEPATH=../data/config" > .env`
* Create directory structure in home directory:
    * mkdir config (for configuration files)
    * mkdir files
    * mkdir files
    * mkdir indexes
    * mkdir input
    * mkdir logs
    * mkdir output
* Add the config files (from existing or sample_config) to config/config_cache/
* Edit base.json to reflect local paths


## Load Initial Data

* Given the extracted dump files, load them
    * __This is where the new loader will be__

* Create a reconciliation token: python ./manage-data.py --new-token
* Bootstrap the system with necessary records: python ./manage-data.py --baseline
* Run the main script to process everything: ./run-all.sh

Or, to do each step by hand in a single thread:

* python ./run-reconcile.py --SOURCE
* python ./merge-metatypes.py
* rm metatypes-\*.json
* mv metatypes.json ../data/files/
* python ./manage-data.py --write-refs
* python ./run-merge.py --SOURCE
* python ./run-export.py

