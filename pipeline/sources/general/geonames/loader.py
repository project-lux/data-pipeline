from pipeline.process.base.loader import Loader
import os
from cromulent import model, vocab
import time

# The zip file has a CSV, but the names don't have languages :(
# Maybe useful for reconciliation though (see LCNAF)
# alternateNamesV2 has langs

#geonameid         : integer id of record in geonames database
#name              : name of geographical point (utf8) varchar(200)
#asciiname         : name of geographical point in plain ascii characters, varchar(200)
#alternatenames    : alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
#latitude          : latitude in decimal degrees (wgs84)
#longitude         : longitude in decimal degrees (wgs84)
#feature class     : see http://www.geonames.org/export/codes.html, char(1)
#feature code      : see http://www.geonames.org/export/codes.html, varchar(10)
#country code      : ISO-3166 2-letter country code, 2 characters
#cc2               : alternate country codes, comma separated, ISO-3166 2-letter country code, 200 characters
#admin1 code       : fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
#admin2 code       : code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80) 
#admin3 code       : code for third level administrative division, varchar(20)
#admin4 code       : code for fourth level administrative division, varchar(20)
#population        : bigint (8 byte int) 
#elevation         : in meters, integer
#dem               : digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
#timezone          : the iana timezone id (see file timeZone.txt) varchar(40)
#modification date : date of last modification in yyyy-MM-dd format


class GnLoader(Loader):     

    def __init__(self, config): 
        Loader.__init__(self, config)
        self.namespace = config['namespace']
        hiers = os.path.join(self.in_path, 'hierarchy.txt')
        if os.path.exists(hiers):
            fh = open(hiers)
            self.child_parent = {}
            for l in fh.readlines():
                (p,c,t) = l.split('\t')
                self.child_parent[c] = p
            fh.close()
        self.total = 12363290

    def load(self):
        allc = os.path.join(self.in_path, 'allCountries.txt')
        fh = open(allc)
        start = time.time()
        x = 0
        for l in fh.readlines():
            x += 1
            stuff = l.split('\t')
            gnid = stuff[0]
            if not gnid in self.out_cache:
                name = stuff[1]
                alt = stuff[3]
                alts = alt.split(',')
                lat = stuff[4]
                lng = stuff[5]
                parent = self.child_parent.get(gnid, None)

                ident = f"{self.namespace}{gnid}"
                top = model.Place(ident=ident,label=name)
                top.identified_by = vocab.PrimaryName(content=name)
                for a in alts:
                    if a != name:
                        top.identified_by = vocab.AlternateName(content=a)
                if lat and lng:
                    top.defined_by = f"POINT ( {lng} {lat} )"
                if parent:
                    top.part_of = model.Place(ident=f"{self.namespace}{parent}")
                data = model.factory.toJSON(top)
                self.out_cache[gnid] = data
                if not x % 10000:
                    t = time.time() - start
                    xps = x/t
                    ttls = self.total / xps
                    print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")
