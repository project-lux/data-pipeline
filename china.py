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

wd_sames = ['Q148']

wd_differents = ['Q20233549','Q942154']
#wd_differents = ['Q13426199','Q29520']

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

all_names = same_names + different_names

encoder = OneHotEncoder(sparse=False)
encoder.fit(np.array(all_names).reshape(-1, 1))

encoded_sames = encoder.transform(np.array(same_names).reshape(-1, 1))
encoded_diffs = encoder.transform(np.array(different_names).reshape(-1, 1))

similarity = cosine_similarity(encoded_sames, encoded_diffs)

highest_similarity = np.max(similarity)
print(f"Highest Similarity: {highest_similarity}")

if highest_similarity > 0.9:
    print("Very high similarity")
elif highest_similarity > 0.7:
    print("High similarity")
elif highest_similarity > 0.5:
    print("Moderate similarity")
elif highest_similarity > 0.3:
    print("Low similarity")
else:
    print("Very low similarity")
