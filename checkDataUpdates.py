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
idmap = cfgs.instantiate_map('idmap')['store']
cfgs.instantiate_all()

checkcaches = ['aat','bnf','geonames','japan','japansh','lcdgt','tgn','ulan','viaf','wikimedia']
remotefiles = {"lcnaf":"https://lds-downloads.s3.amazonaws.com/authorities/names.madsrdf.jsonld.json","lcsh":"https://lds-downloads.s3.amazonaws.com/authorities/subjects.madsrdf.jsonld.json",
    "wikidata":"https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz","dnb:sachbegriff":"https://data.dnb.de/opendata/authorities-gnd-sachbegriff_lds.jsonld.gz", "dnb:entityfacts":"https://data.dnb.de/opendata/authorities-gnd_entityfacts.jsonld.gz", "dnb:macs":"https://data.dnb.de/opendata/macs.nt.gz",
    "fast":"https://researchworks.oclc.org/researchdata/fast/FASTAll.marcxml.zip","geonames:allCountries":"https://download.geonames.org/export/dump/allCountries.zip", "geonames:altNames":"https://download.geonames.org/export/dump/alternateNamesV2.zip","geonames:hierarchies":"https://download.geonames.org/export/dump/hierarchy.zip",
    "wof":"https://data.geocode.earth/wof/dist/sqlite/whosonfirst-data-admin-latest.db.bz2"}
localfiles = {"dnb:sachbegriff":"authorities-gnd-sachbegriff_lds.jsonld.gz","dnb:entityfacts":"authorities-gnd_entityfacts.jsonld.gz","dnb:macs":"macs.nt","fast":"FASTAll.marcxml.zip","geonames:allCountries":"allCountries.zip","geonames:altNames":"alternateNamesV2.zip","geonames:hierarchies":"hierarchy.zip",
	"lcnaf":"names.madsrdf.jsonld.gz","lcsh":"subjects.madsrdf.jsonld.gz","wikidata":"latest-all.json.gz","wof":"latest_wof.db"}
failed = []

def get_remote_modified_time(url, source):
	disable_warnings(InsecureRequestWarning)
	try:
		r = requests.head(url)
	except:
		print(f"***{source} failed at requesting the URL")
		failed.append(source)
		r = None
	if r and r.status_code == 200:
		head = dict(r.headers)
		try:
			last_mod = head.get("Last-Modified","")
		except:
			print(f"***{source} failed at getting the last_mod time")
			failed.append(source)
			last_mod = None
		if last_mod:
			dtobj = datetime.strptime(last_mod, '%a, %d %b %Y %H:%M:%S GMT')
	else:
		print(f"***{source} failed, URL didn't return a 200")
		failed.append(source)
		dtobj = None
	return dtobj

def get_local_modified_time(file, source):

	if file == "macs.nt":
		filepath = f"/data-io2/input/{source}/old/{file}"
	else:
		filepath = f"/data-io2/input/{source}/{file}"
	try:
		timestamp = os.path.getmtime(filepath)
	except:
		print(f"***{source} failed at getting timestamp from local file")
		failed.append(source)
		timestamp = None
	if timestamp:
		try:
			datestamp = datetime.fromtimestamp(timestamp)
		except:
			print(f"***{source} failed at getting datestamp from local file timestamp")
			failed.append(source)
			datestamp = None
	return datestamp

def build_dicts(fileset, local=""):
	times = {}
	for source, files in fileset.items():
		if local == "Y":
			if ":" in source:
				src, fle = source.split(":")
				timestamp = get_local_modified_time(files, src)
				if timestamp:
					times[source] = timestamp
			else:
				timestamp = get_local_modified_time(files, source)
				if timestamp:
					times[source] = timestamp
		elif local == "":
			timestamp = get_remote_modified_time(files, source)
			if timestamp:
				times[source] = timestamp
	return times

localtimes = build_dicts(localfiles, local="Y")
remotetimes = build_dicts(remotefiles,local="")
print("*****Checking local and remote times for dump files*****")

for keys, values in localtimes.items():
	try:
		remotetime = remotetimes[keys]
		print(f"{keys} has local time of {values}")
		print(f"{keys} has remotetime of {remotetime}")
		if remotetime > values:
			diff = remotetime - values
			print(f"{keys} needs updating. Remote is newer than local. Difference is {diff}.")
			if ":" in keys:
				src, ext = keys.split(":")
				print(f"Local dumpfile path is: /data-io2/input/{src}/{localfiles[keys]}")
			else:
				print(f"Local dumpfile path is: /data-io2/input/{keys}/{localfiles[keys]}")
			print(f"Remote dumpfile path is: {remotefiles[keys]}")
		elif remotetime <= values:
			diff = values - remotetime
			diffsec = int(diff.total_seconds())
			if diffsec == 0:
				print(f"{keys} doesn't need updating. There is no difference between {keys} local and remote.")
			else:
				print(f"{keys} doesn't need updating. Local is newer than remote. Different is {diff}")
	except:
		print(f"***{keys} failed at comparing localtimes and remotetimes")
		failed.append(keys)


print("*****Checking datacache times and comparing to sample record times*****")

cachetimes = {}
samples = {}
cacheslice = []

for cache in checkcaches:
	datacache = cfgs.external[cache]['datacache']
	cachets = datacache.latest()
	if cachets.startswith("0000"):
		#cache is likely empty
		print(f"***{cache} failed because latest time begins with 0000")
		failed.append(cache)
	else:
		cachedt = datetime.fromisoformat(cachets)
		cachetimes[cache] = cachedt
		#build a slice
		for s in datacache.iter_keys_slice(5,1000):
			cacheslice.append(s)
		samplerecs = random.sample(cacheslice, 3)
		samples[cache] = samplerecs

samplerecs = {}
checkcaches = [a for a in checkcaches if a not in failed]
for cache in checkcaches:
	datacache = cfgs.external[cache]['datacache']
	samplelist = samples[cache]
	for sample in samplelist:
		try:
			rec = datacache[sample]
			samplerecs[cache] = rec
		except:
			print(f"couldn't return {sample} from datacache {cache}")
			rec = None 
		if rec:
			rectime = rec['record_time']
			#returns a datetime.datetime
			if cache not in failed:
				if rectime > cachetimes[cache]:
					print(f"Cache needs updating. Cache {cache} sample rec {sample} has insert time of {rectime}, and is newer than cache time of {cachetimes[cache]}.")
					break
				else:
					print(f"Cache doesn't need updating. Cache {cache} has time of {cachetimes[cache]}, is newer than rec {cache}:{sample}.")
					break

failed = list(set(failed))
print(f"failed sources: {failed}")

#FIXME: fail needs to be external source exactly
for fail in failed:
	if fail in ['lcdgt','japansh']:
		continue
	try:
		fetcher = cfgs.external[fail]['fetcher']
	except:
		print(f"{fail} doesn't have a fetcher")
		fetcher = None
	if fetcher:
		pass


#If not, then fetch the rec and compare data against the data in the cache to see if they're different.

