import os
import json 
import requests
from ..mapper import ArticMapper
from cromulent import model
model.factory.auto_assign_id = False
model.factory.base_dir = "/Users/kd736/Desktop/artic/"
model.factory.base_url = "https://api.artic.edu/api/v1/agents"

# session = requests.Session()

# def get_jobs():
#     url = "https://api.artic.edu/api/v1/galleries" 
#     first_page = session.get(url).json()
#     yield first_page
#     num_pages = first_page['pagination']['total_pages']

#     for page in range(2, num_pages + 1):
#         next_page = session.get(url, params={'page': page}).json()
#         yield next_page

# galleries = {}
# for page in get_jobs():
# 	data = page['data']
# 	for d in data:
# 		ident = d['id']
# 		title = d['title']
# 		galleries[ident] = title

artistlist = []
for file in os.listdir('/Users/kd736/Desktop/artic/HumanMadeObject'):
	with open(f'/Users/kd736/Desktop/artic/HumanMadeObject/{file}') as file:
		record = json.loads(file.read())
		try:
			artist = record['produced_by']['carried_out_by']
		except:
			artist = None
		if artist:
			for a in artist:
				art_id = a['id']
				art_id = art_id.split('/')[-1]
				artistlist.append(art_id)

mapper = ArticMapper()
jsonlist = []
for file in os.listdir('/Users/kd736/Desktop/artic/artists'):
	with open(f'/Users/kd736/Desktop/artic/artists/{file}') as file:
		record = json.loads(file.read())
		rec_id = str(record['id'])
		if rec_id in artistlist:
			top = mapper.transform(record)
			jsonlist.append(top)

for value in jsonlist:
	model.factory.toFile(value)
