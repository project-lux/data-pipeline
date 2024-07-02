#Pipeline #439
#fail needs to be external source exactly
#should iterate over files in data-io, not hardcode--needs directory cleanup

import os
import requests
import random
from datetime import datetime
from pipeline.config import Config
from dotenv import load_dotenv
from urllib3.exceptions import InsecureRequestWarning
from urllib3 import disable_warnings

load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.instantiate_all()

checkcaches = ['aat','bnf','geonames','japan','japansh','lcdgt','tgn','ulan','viaf','wikimedia']
remote_files = {
	"lcnaf": "https://lds-downloads.s3.amazonaws.com/authorities/names.madsrdf.jsonld.json",
	"lcsh": "https://lds-downloads.s3.amazonaws.com/authorities/subjects.madsrdf.jsonld.json",
	"wikidata": "https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz",
	"dnb:sachbegriff": "https://data.dnb.de/opendata/authorities-gnd-sachbegriff_lds.jsonld.gz",
	"dnb:entityfacts": "https://data.dnb.de/opendata/authorities-gnd_entityfacts.jsonld.gz",
	"dnb:macs": "https://data.dnb.de/opendata/macs.nt.gz",
	"fast": "https://researchworks.oclc.org/researchdata/fast/FASTAll.marcxml.zip",
	"geonames:allCountries": "https://download.geonames.org/export/dump/allCountries.zip",
	"geonames:altNames": "https://download.geonames.org/export/dump/alternateNamesV2.zip",
	"geonames:hierarchies": "https://download.geonames.org/export/dump/hierarchy.zip",
	"wof": "https://data.geocode.earth/wof/dist/sqlite/whosonfirst-data-admin-latest.db.bz2"
}
local_files = {
	"dnb:sachbegriff": "authorities-gnd-sachbegriff_lds.jsonld.gz",
	"dnb:entityfacts": "authorities-gnd_entityfacts.jsonld.gz",
	"dnb:macs": "macs.nt",
	"fast": "FASTAll.marcxml.zip",
	"geonames:allCountries": "allCountries.zip",
	"geonames:altNames": "alternateNamesV2.zip",
	"geonames:hierarchies": "hierarchy.zip",
	"lcnaf": "names.madsrdf.jsonld.gz",
	"lcsh": "subjects.madsrdf.jsonld.gz",
	"wikidata": "latest-all.json.gz",
	"wof": "latest_wof.db"
}


failed = []

def get_remote_modified_time(url, source):
	disable_warnings(InsecureRequestWarning)
	try:
		r = requests.head(url)
		r.raise_for_status()
	except requests.exceptions.RequestException as e:
		print(f"***{source} failed at requesting the URL: {e}")
		failed.append(source)
		return None

	head = dict(r.headers)
	last_mod = head.get("Last-Modified", "")
	if last_mod:
		dtobj = datetime.strptime(last_mod, '%a, %d %b %Y %H:%M:%S GMT')
		return dtobj
	else:
		print(f"***{source} failed at getting the last_mod time")
		failed.append(source)
		return None


def get_local_modified_time(file, source):

	if file == "macs.nt":
		filepath = f"/data-io2/input/{source}/old/{file}"
	else:
		filepath = f"/data-io2/input/{source}/{file}"

	try:
		timestamp = os.path.getmtime(filepath)
		datestamp = datetime.fromtimestamp(timestamp)
		return datestamp
	except OSError as e:
		print(f"***{source} failed at getting timestamp from local file: {e}")
		failed.append(source)
		return None

def build_dicts(fileset, local=False):
	times = {}
	for source, files in fileset.items():
		if local == True:
			if ":" in source:
				source = source.split(":",1)[0]
			timestamp = get_local_modified_time(files, source)
		timestamp = get_remote_modified_time(files, source)
		times[source] = timestamp
	return times


def check_local_and_remote_times(local_files, remote_files):
	local_times = build_dicts(local_files, local=True)
	remote_times = build_dicts(remote_files)

	print("*****Checking local and remote times for dump files*****")

	for key, local_time in local_times.items():
		try:
			remote_time = remote_times[key]
			print(f"{key} has local time of {local_time}")
			print(f"{key} has remotetime of {remote_time}")
			if remote_time > local_time:
				diff = remote_time - local_time
				print(f"{key} needs updating. Remote is newer than local. Difference is {diff}.")
				if ":" in key:
					src, ext = key.split(":")
					print(f"Local dumpfile path is: /data-io2/input/{src}/{local_files[key]}")
				else:
					print(f"Local dumpfile path is: /data-io2/input/{key}/{local_files[key]}")
				print(f"Remote dumpfile path is: {remote_files[key]}")
			elif remote_time <= local_time:
				diff = local_time - remote_time
				diffsec = int(diff.total_seconds())
				if diffsec == 0:
					print(f"{key} doesn't need updating. There is no difference between {key} local and remote.")
				else:
					print(f"{key} doesn't need updating. Local is newer than remote. Difference is {diff}")
		except:
			print(f"***{key} failed at comparing localtimes and remotetimes")

def check_datacache_times(check_caches: list) -> None:
	print("*****Checking datacache times and comparing to sample record times*****")

	cachetimes = {}
	samples = {}
	cacheslice = []

	for cache in check_caches:
		datacache = cfgs.external[cache]['datacache']
		cachets = datacache.latest()
		if cachets.startswith("0000"):
			print(f"***{cache} failed because latest time begins with 0000")
			continue
		
		cachedt = datetime.fromisoformat(cachets)
		cachetimes[cache] = cachedt

		for s in datacache.iter_keys_slice(5, 1000):
			cacheslice.append(s)
		
		samplerecs = random.sample(cacheslice, 3)
		samples[cache] = samplerecs

	samplerecs = {}
	check_caches = [a for a in check_caches if a not in failed]
	
	for cache in check_caches:
		datacache = cfgs.external[cache]['datacache']
		samplelist = samples[cache]
		
		for sample in samplelist:
			try:
				rec = datacache[sample]
				samplerecs[cache] = rec
			except KeyError:
				print(f"couldn't return {sample} from datacache {cache}")
				rec = None 
			
			if rec:
				rectime = rec['record_time']
				if cache not in failed:
					if rectime > cachetimes[cache]:
						print(f"Cache needs updating. Cache {cache} sample rec {sample} has insert time of {rectime}, and is newer than cache time of {cachetimes[cache]}.")
						break
					else:
						print(f"Cache doesn't need updating. Cache {cache} has time of {cachetimes[cache]}, is newer than rec {cache}:{sample}.")
						break

def fetch_failed_sources(failed):
	for fail in failed:
		if fail in ['lcdgt', 'japansh']:
			continue
		try:
			fetcher = cfgs.external[fail]['fetcher']
		except KeyError:
			print(f"{fail} doesn't have a fetcher")
			fetcher = None
		if fetcher:
			pass


def main():
	check_local_and_remote_times(local_files, remote_files)
	check_datacache_times(check_caches)
	fetch_failed_sources(failed)
	print(f"failed sources: {failed}")

if __name__ == "__main__":
	main()


