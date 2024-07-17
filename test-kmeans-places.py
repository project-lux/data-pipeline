from dotenv import load_dotenv
from pipeline.config import Config
import re
import os
import numpy as np
from sklearn.cluster import KMeans


load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

cfgs.debug_reconcile = False

point_pattern = re.compile(r'(-?\d+\.\d+|-?\d+)\s*,?\s*(-?\d+\.\d+|-?\d+)')
polygon_pattern = re.compile(r'POLYGON\s*\(\(\s*([-?\d+\.\d+\s*,?]+)\s*\)\)')


#Mexico
#equivs = idmap['https://lux.collections.yale.edu/data/place/bbd6d968-c465-4f56-b779-ac7b7196083c']

#United States
equivs = idmap['https://lux.collections.yale.edu/data/place/f14804ea-6bd1-4bfb-9394-6f5428c83c34']

coords = []

for e in equivs:
	if e.startswith("__"):
		continue
	if "yale" in e:
		#library recs don't have defined_by
		continue
	(base, qua) = cfgs.split_qua(e)
	(src, ident) = cfgs.split_uri(base)
	#acquire recordcache rec
	try:
		rec = src['acquirer'].acquire(ident,rectype=qua)
	except:
		rec = None

	#get current recs coords
	if rec:
		defined_by = rec['data'].get('defined_by')
		if defined_by:
			if defined_by.startswith("POINT"):
				coordinates = point_pattern.findall(defined_by)
				longitude, latitude = map(float, coordinates[0].strip())
				coords.append([longitude, latitude])
			elif defined_by.startswith("POLYGON"):
				coordinates = polygon_pattern.findall(defined_by)
				coordinates_str = coordinates[0].split(',')
				for coord in coordinates_str:
					# Split each coordinate pair by whitespace
					longitude, latitude = map(float, coord.strip().split())
					# Append the coordinates to the list
					coords.append([longitude, latitude])


coordinates_array = np.array(coords)
print(coordinates_array)


# kmeans = KMeans(n_clusters=2, random_state=0)
# kmeans.fit(coordinates_array)

# # Predict clusters
# y_kmeans = kmeans.predict(coordinates_array)

	#get parent recs coords
	# part_of = rec.get("part_of")
	# if part_of:
	# 	for p in part_of:
	# 		pid = p.get('id')
	# 		if pid:
	# 			(src, ident) = cfgs.split_uri(pid)
	# 			parent = src['acquirer'].acquire(ident)
	# 			defined_by = parent.get('defined_by')
	# 			if defined_by:
	# 				coordinates = coordinate_pattern.findall(defined_by)
	# 				longitude, latitude = map(float, coordinates) 
	# 				# Create a NumPy array from the coordinates
	# 				coordinates_array = np.array([longitude, latitude])





