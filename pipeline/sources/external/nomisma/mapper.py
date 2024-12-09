#http://nomisma.org/ontology
#7468 records total

from pipeline.process.base.mapper import Mapper
from pipeline.process.utils.mapper_utils import make_datetime
from cromulent import model, vocab

class NomismaMapper(Mapper):

	def __init__(self, config):
		Mapper.__init__(self, config)
		self.namespace = config['namespace']

	def transform(self, record, rectype=None, reference=False):
		recid = record['identifier']
		graph = record['data'].get('@graph',[])
		if graph == []:
			#can't process
			return None
		if type(graph) != []:
			graph = list(graph)
		for r in graph:
			if r['@id'] == f"nm:{recid}":
				rectype = r['@type']
				if type(rectype) != list:
					rectype = [rectype]
				for t in rectype:
					if t in ["nmo:Mint","nmo:Region"]:
						topcls = model.Place
						top = topcls(ident=f"{self.namespace}{recid}.jsonld")
						self.handle_common(r, recid, top, topcls)
						break
					elif t in ["wordnet:Deity","foaf:Person"]:
						topcls = model.Person
						top = topcls(ident=f"{self.namespace}{recid}.jsonld")
						self.handle_person(r, graph, recid, top)
						self.handle_common(r, recid, top, topcls)
						break
					elif t in ['nmo:Collection','rdac:Family','foaf:Organization','nmo:Ethnic','foaf:Group']:
						topcls = model.Group 
						top = topcls(ident=f"{self.namespace}{recid}.jsonld")
						self.handle_common(r, recid, top, topcls)
						self.handle_group(r, recid, top)
						break
					elif t in ['nmo:Denomination','nmo:Material','nmo:ObjectType','nmo:NumismaticTerm','nmo:TypeSeries']:
						topcls = model.Type 
						top = topcls(ident=f"{self.namespace}{recid}.jsonld")
						self.handle_common(r, recid, top, topcls)
						break

		if topcls == model.Place:
			self.handle_geospatial(graph, recid, top)

		data = model.factory.toJSON(top)
		return {'identifier': recid, 'data': data, 'source': 'nomisma'}

	def handle_common(self, r, recid, top, topcls):
		preflbl = r.get('skos:prefLabel',[])
		if type(preflbl) != list:
			preflbl = [preflbl]
		if preflbl:
			for pref in preflbl:
				preflang = pref.get('@language','')
				prefval = pref.get('@value','')
				if prefval:
					nm = vocab.PrimaryName(content=prefval)
					lango = self.process_langs.get(preflang, None)
					if lango:
						nm.language = lango
					top.identified_by = nm
		
		#altlabel
		altlbl = r.get('skos:altLabel',[])
		if type(altlbl) != list:
			altlbl = [altlbl]
		if altlbl:
			for alt in altlbl:
				altlang = alt.get('@language','')
				altval = alt.get('@value','')
				if altval:
					am = vocab.AlternateName(content=altval)
					if altlang:
						langa = self.process_langs.get(altlang, None)
						if langa:
							am.language = langa 
					top.identified_by = am

		description = r.get('skos:definition',[])
		if type(description) != list:
			description = [description]
		if description:
			for d in description:
				dlang = d['@language']
				dval = d['@value']
				desc = vocab.Note(content=dval)
				lango = self.process_langs.get(dlang, None)
				if lango:
					desc.language = lango
				top.referred_to_by = desc

		scopeNote = r.get('skos:scopeNote',[])
		if type(scopeNote) != list:
			scopeNote = [scopeNote]
		if scopeNote:
			for note in scopeNote:
				nlang = note['@language']
				nval = note['@value']
				scope = vocab.Note(content=nval)
				lango = self.process_langs.get(nlang, None)
				if lango:
					scope.language = lango
				top.referred_to_by = scope
		
		equivs = ['skos:closeMatch','skos:exactMatch']
		for e in equivs:
			close = r.get(e,[])
			if type(close) != list:
				close = [close]
			if close:
				for c in close:
					if topcls == model.Place:
						top.equivalent = model.Place(ident=c['@id'])
					elif topcls == model.Person:
						top.equivalent = model.Person(ident=c['@id'])
					elif topcls == model.Type:
						top.equivalent = model.Type(ident=c['@id'])
					elif topcls == model.Group:
						top.equivalent = model.Group(ident=c['@id'])
		
		if topcls in [model.Type, model.Place]:
			broader = r.get('skos:broader',[])
			if type(broader) != list:
				broader = [broader]
			if broader:
				for b in broader:
					try:
						bid = b.get('@id','')
						bid1 = bid.split('nm:')[-1]
					except:
						#only do it if Nomisma refs
						continue
					if bid1:
						if topcls == model.Place:
							top.part_of = model.Place(ident=f"{self.namespace}{bid1}.jsonld")
						else:
							top.broader = model.Type(ident=f"{self.namespace}{bid1}.jsonld")
		
		homepage = r.get('foaf:homepage',[])
		if type(homepage) != list:
			homepage = [homepage]
		if homepage:
			#but only accept one
			addr = homepage[0]['@id']
			lo = model.LinguisticObject(label="Website Text")
			do = vocab.WebPage(label="Home Page")            
			do.access_point = model.DigitalObject(ident=addr)
			lo.digitally_carried_by = do
			top.subject_of = lo


	def handle_geospatial(self, graph, recid, top):
		for r in graph:
			if r['@id'] == f"{self.namespace}{recid}#this":
			#won't work for Region
				if r['@type'] == "geo:SpatialThing":
					if r['geo:lat']:
						latval = r['geo:lat'].get('@value','')
					if r['geo:long']:
						longval = r['geo:long'].get('@value','')
					if latval and longval:
						top.defined_by = f"POINT ( {longval} {latval} )"


	def handle_person(self, r, graph, recid, top):
		#nomisma has membership dates which we don't currently do anything with in LUX
		membership = r.get('org:hasMembership',[])
		if type(membership) != list:
			membership = [membership]
		for mem in membership:
			memid = mem.get('@id','')
			if memid:
				for g in graph:
					if g['@id'] == memid:
						morg = g.get('org:organization',{})
						if morg:
							try:
								mid = morg.get('@id','')
								mid1 = mid.split('nm:')[-1]
							except:
								#only do it if Nomisma refs
								continue
							if mid1:
								top.member_of = model.Group(ident=f"{self.namespace}{mid1}.jsonld")
		memberOf = r.get('org:memberOf',[])
		if type(memberOf) != list:
			memberOf = [memberOf]
		if memberOf:
			for m in memberOf:
				meid = m.get('@id','')
				if meid:
					try:
						meid1 = meid.split('nm:')[-1]
					except:
						#only do it if Nomisma refs
						continue
					if meid1:
						top.member_of = model.Group(ident=f"{self.namespace}{meid1}.jsonld")

		bgraph = r.get('bio:birth','')
		if bgraph:
			for g in graph:
				if g['@id'] == f'{self.namespace}{recid}#birth':
					bdate = g.get('dcterms:date',{})
					if bdate:
						if bdate['@type'] == "xsd:gYear":
							#only process years, dunno what other values might exisy
							bval = bdate['@value']
							try:
								b,e = make_datetime(bval)
							except:
								b = None
							if b:
								birth = model.Birth()
								ts = model.TimeSpan()
								ts.begin_of_the_begin = b
								ts.end_of_the_end = e
								ts.identified_by = vocab.DisplayName(content=bval)
								birth.timespan = ts
								top.born = birth
		dgraph = r.get('bio:death','')
		if dgraph:
			for g in graph:
				if g['@id'] == f'{self.namespace}{recid}#death':
					ddate = g.get('dcterms:date',{})
					if ddate:
						if ddate['@type'] == "xsd:gYear":
							dval = ddate['@value']
							try:
								b,e = make_datetime(dval)
							except:
								b = None
							if b:
								death = model.Death()
								ts = model.TimeSpan()
								ts.begin_of_the_begin = b
								ts.end_of_the_end = e
								ts.identified_by = vocab.DisplayName(content=dval)
								death.timespan = ts
								top.death = death

	def handle_group(self, r, recid, top):
		fdate = r.get('nmo:hasStartDate',{})
		if fdate:
			if fdate['@type'] == "xsd:gYear":
				fval = fdate['@value']
				try:
					b,e = make_datetime(fval)
				except:
					b = None
				if b:
					formed = model.Formation()
					ts = model.TimeSpan()
					ts.begin_of_the_begin = b
					ts.end_of_the_end = e
					ts.identified_by = vocab.DisplayName(content=fval)
					formed.timespan = ts
					top.formed_by = formed
		edate = r.get('nmo:hasEndDate',{})
		if edate:
			if edate['@type'] == "xsd:gYear":
				edval = edate['@value']
				try:
					b,e = make_datetime(edval)
				except:
					b = None
				if b:
					dissolved = model.Dissolution()
					ts = model.TimeSpan()
					ts.begin_of_the_begin = b
					ts.end_of_the_end = e
					ts.identified_by = vocab.DisplayName(content=edval)
					dissolved.timespan = ts
					top.dissolved_by = dissolved


