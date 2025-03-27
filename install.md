
# INSTALL


## Install system (Ubuntu)

From a brand new server (e.g. an instance in spinup if you're at Yale)

As root:
* apt install redis
* apt install postgresql
* Then follow the instructions here: https://www.postgresql.org/download/linux/ubuntu/ 
  to get the latest postgres

* apt install python3-pip
* apt install python3.10-venv
* pip install --upgrade pip
* su - postgres
* createuser <user>
* createdb <user>


## Install system (Mac)

Or on Macos:

* Install postgres: I use https://postgresapp.com/
* Install pgadmin: I use https://www.pgadmin.org/download/pgadmin-4-macos/
* Install homebrew: I use the .pkg installer linked from https://brew.sh/
* Update homebrew: brew update
* Install redis:  brew install redis
* Set both postgres and redis to start at login

## Install LUX

As user:

* mkdir lux
* cd lux
* python3 -m venv ENV
* source ENV/bin/activate
* pip install lux-pipeline

This won't work yet. Currently
* git clone https://github.com/project-lux/data-pipeline.git
* cd data-pipeline
* git checkout rob_refactor
* pip install -e .
* pip install -r requirements.txt
* cd ..

* mkdir data
* cd data
* lux initialize
* lux testinstall

### Download data

* Download exported LUX baseline: lux download --source all --type export --max_workers 10
* Download source datasets: lux download --source all --type records --max_workers 10
*     Use at least 10, more if the CPU and network can handle it


### Load data to cache

* lux load --source all --type export --max_workers 10


