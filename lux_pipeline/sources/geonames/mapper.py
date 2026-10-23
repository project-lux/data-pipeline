from lux_pipeline.process.base.mapper import Mapper
from cromulent import model, vocab
from lxml import etree

#<rdf:RDF>
#  <gn:Feature>
#    <properties here>

class GnMapper(Mapper):

    def __init__(self, config):
        Mapper.__init__(self, config)
        self.namespaces = {
            "gn":"http://www.geonames.org/ontology#", 
            "rdf":"http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "wgs":"http://www.w3.org/2003/01/geo/wgs84_pos#",
            "rdfs":"http://www.w3.org/2000/01/rdf-schema#"
        }

    def guess_type(self, data):
        # uhh, France?
        return model.Place

    def transform(self, record, rectype, reference=False):
        if rectype != "Place":
            # print(f"GeoNames asked for a non-Place: {rectype}; nothing to do")
            return None
        try:
            xml = record['data']['value']
        except:
            # Could be JSON loaded from file
            data = record['data']
            if 'id' in data and 'type' in data and data['type'] == 'Place':
                return {'identifier': record['identifier'], 'data': data, 'source': 'geonames'}
            else:
                return None

        try:
            dom = etree.XML(xml.encode('utf-8'))
        except:
            # print(f"Broken XML in geonames {record['identifier']}")
            return None
        nss = self.namespaces
        feature = dom.xpath('/rdf:RDF/gn:Feature', namespaces=nss)[0] # if doesn't exist, v broken
        ident = self.to_plain_string(feature.xpath('./@rdf:about', namespaces=nss)[0]) # ditto
        ident = ident.strip()
        if ident.endswith('/'):
            ident = ident[:-1]
        name = [self.to_plain_string(n) for n in feature.xpath('./gn:name/text()', namespaces=nss)]
        offNames = feature.xpath('./gn:officialName', namespaces=nss) # element, as need xml:lang attrib
        altNames = feature.xpath('./gn:alternateName', namespaces=nss)
        shortNames = feature.xpath('./gn:shortName', namespaces=nss) # ???

        fclass = [self.to_plain_string(fc) for fc in feature.xpath('./gn:featureClass/@rdf:resource', namespaces=nss)] # ignore ... too many to be bothered
        fcode = [self.to_plain_string(fc) for fc in feature.xpath('./gn:featureCode/@rdf:resource', namespaces=nss)]

        ccode = [self.to_plain_string(cc) for cc in feature.xpath('./gn:countryCode/text()', namespaces=nss)]
        lat = [self.to_plain_string(lt) for lt in feature.xpath('./wgs:lat/text()', namespaces=nss)]
        lng = [self.to_plain_string(lg) for lg in feature.xpath('./wgs:long/text()', namespaces=nss)]
        parentFeature = [self.to_plain_string(pf) for pf in feature.xpath('./gn:parentFeature/@rdf:resource', namespaces=nss)]
        parentCountry = [self.to_plain_string(pc) for pc in feature.xpath('./gn:parentCountry/@rdf:resource', namespaces=nss)]
        seeAlso = [self.to_plain_string(sa) for sa in feature.xpath('./rdfs:seeAlso/@rdf:resource', namespaces=nss)]


        pnames = {}
        anames = {}
        if name:
            name = name[0]
        for onm in offNames:
            lang = onm.xpath('./@xml:lang')
            lang = '' if not lang else self.to_plain_string(lang[0])
            txt = self.to_plain_string(onm.xpath('./text()')[0])
            if not lang or lang in self.process_langs:
                pnames[lang] = txt

        for onm in altNames:
            lang = onm.xpath('./@xml:lang')
            lang = '' if not lang else self.to_plain_string(lang[0])
            txt = self.to_plain_string(onm.xpath('./text()')[0])
            if not lang or lang in self.process_langs:
                try:
                    anames[lang].append(txt)
                except:
                    anames[lang] = [txt]
        if not name:
            if 'en' in pnames:
                name = pnames['en']
            elif 'en' in anames:
                name = anames['en'][0]
            elif '' in pnames:
                name = pnames['']
            elif pnames:
                # random pname
                name = pnames.values()[0]
            elif anames:    
                # random aname
                name = anames.values()[0][0]
            else:
                print(f"No name found for {ident}")

        top = model.Place(ident=ident,label=name)

        for pl,pv in pnames.items():
            nm = vocab.PrimaryName(content=pv)
            if pl:
                nm.language = self.process_langs[pl]
            top.identified_by = nm

        for al, avs in anames.items():
            for av in avs:
                nm = vocab.AlternateName(content=av)
                if al:
                    nm.language = self.process_langs[al]
                top.identified_by = nm

        if lat and lng:
            lat = lat[0]
            lng = lng[0]
            top.defined_by = f"POINT ( {lng} {lat} )"

        if parentFeature:
            for p in parentFeature:
                top.part_of = model.Place(ident=p)
        elif parentCountry:
            for p in parentCountry:
                top.part_of = model.Place(ident=p)

        if seeAlso:
            for s in seeAlso:
                top.equivalent = model.Place(ident=s, label=name)

        data = model.factory.toJSON(top)
        recid = record['identifier']
        if recid.endswith('/'):
            recid = recid[:-1]
        return {'identifier': recid, 'data': data, 'source': 'geonames'}
