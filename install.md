
# INSTALL

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



