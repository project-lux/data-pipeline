import numpy as np 
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics.pairwise import cosine_similarity
from dotenv import load_dotenv
from pipeline.config import Config
import os


load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()


wdrc = cfgs.external['wikidata']['recordcache']

wd_sames = ['Q148','Q13426199','Q29520']

wd_differents = ['Q20233549','Q942154']

same_names = []

different_names = []
#get primary names, stuff in a list

def get_names(reclist, same=False):
	for name in reclist:
		rec = wdrc[f"{name}##quaPlace"]
		data = rec['data']
		idents = data.get("identified_by")
		if idents:
			for i in idents:
				cxns = i.get("classified_as")
				if cxns:
					for c in cxns:
						if c['id'] and c['id'] == "http://vocab.getty.edu/aat/300404670":
							if same == True:
								same_names.append(i['content'])
							else:
								different_names.append(i['content'])

get_names(wd_sames,same=True)
get_names(wd_differents)

encoder_same = OneHotEncoder(sparse=False)
encoder_diff = OneHotEncoder(sparse=False)

encoded_sames = encoder_same.fit_transform(np.array(same_names).reshape(-1, 1))
encoded_diffs = encoder_diff.fit_transform(np.array(different_names).reshape(-1, 1))


similarity = cosine_similarity(encoded_sames, encoded_diffs)

print(f"Similarity is: {similarity}")
