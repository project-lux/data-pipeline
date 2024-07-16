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

coordinate_pattern = re.compile(r'(-?\d+\.\d+|-?\d+)\s*,?\s*(-?\d+\.\d+|-?\d+)')

equivs = idmap['https://lux.collections.yale.edu/data/place/bbd6d968-c465-4f56-b779-ac7b7196083c']


for e in equivs:
	if e.startswith("__"):
		continue
	(base, qua) = cfgs.split_qua(e)
	(src, ident) = cfgs.split_uri(base)
	#acquire recordcache rec
	rec = src['acquirer'].acquire(ident)

	#get current recs coords
	defined_by = rec.get('defined_by')
	if defined_by:
		coordinates = coordinate_pattern.findall(defined_by)
		longitude, latitude = map(float, coordinates) 
		# Create a NumPy array from the coordinates
		coordinates_array = np.array([longitude, latitude])

print(len(coordinates_array))


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





