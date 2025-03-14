
from lux_pipeline.process.base.loader import Loader
import os
import time
import tarfile

#
# An empty record looks like this:
# <person:person path="/0009-0009-9733-1001/person">
#     <person:name visibility="public" path="0009-0009-9733-1001">
#         <common:created-date>2023-04-21T05:28:48.830Z</common:created-date>
#         <common:last-modified-date>2023-04-21T05:28:48.830Z</common:last-modified-date>
#         <personal-details:given-names>Shyamala</personal-details:given-names>
#         <personal-details:family-name>A</personal-details:family-name>
#     </person:name>
#     <other-name:other-names path="/0009-0009-9733-1001/other-names"/>
#     <researcher-url:researcher-urls path="/0009-0009-9733-1001/researcher-urls"/>
#     <email:emails path="/0009-0009-9733-1001/email"/>
#     <address:addresses path="/0009-0009-9733-1001/address"/>
#     <keyword:keywords path="/0009-0009-9733-1001/keywords"/>
#     <external-identifier:external-identifiers path="/0009-0009-9733-1001/external-identifiers"/>
# </person:person>
# <activities:activities-summary path="/0009-0009-9733-1001/activities">
#     <activities:distinctions path="/0009-0009-9733-1001/distinctions"/>
#     <activities:educations path="/0009-0009-9733-1001/educations"/>
#     <activities:employments path="/0009-0009-9733-1001/employments"/>
#     <activities:fundings path="/0009-0009-9733-1001/fundings"/>
#     <activities:invited-positions path="/0009-0009-9733-1001/invited-positions"/>
#     <activities:memberships path="/0009-0009-9733-1001/memberships"/>
#     <activities:peer-reviews path="/0009-0009-9733-1001/peer-reviews"/>
#     <activities:qualifications path="/0009-0009-9733-1001/qualifications"/>
#     <activities:research-resources path="/0009-0009-9733-1001/research-resources"/>
#     <activities:services path="/0009-0009-9733-1001/services"/>
#     <activities:works path="/0009-0009-9733-1001/works"/>
# </activities:activities-summary>


class OrcidLoader(Loader):

    def should_load(self, data):
        # Only care if there are one or more fields beyond name and orcid
        # And then only things we really care about, which is -not- works

        # Biography
        if data.find(b'<person:biography') != -1: return True

        # Identifiers
        if data.find(b'/researcher-urls"/>') == -1: return True
        if data.find(b'/email"/>') == -1: return True
        # if data.find(b'/address"/>') == -1: return True
        if data.find(b'/keywords"/>') == -1: return True
        if data.find(b'/external-identifiers"/>') == -1: return True

        # Links
        if data.find(b'/employments"/>') == -1: return True
        if data.find(b'/fundings"/>') == -1: return True
        if data.find(b'/memberships"/>') == -1: return True

        # Otherwise we don't really care?
        return False

    
    def load(self):
        # This loads from annual summary dump file.
        # It strips records with only a name
        # dump file is a single tar.gz of files

        tf = tarfile.open(self.in_path, "r:gz")
        nxt = tf.next()
        x = 0
        done_x = 0
        start = time.time()
        ttl = 16000000
        while nxt is not None:
            if nxt.name.endswith('xml'):
                rech = tf.extractfile(nxt)
                ident = nxt.name.rsplit('/', 1)[-1].replace('.xml', '')
            else:
                nxt = tf.next()
                continue            
            data = rech.read()
            rech.close()
            if self.should_load(data):
                done_x += 1
                self.out_cache[ident] = {"xml": data.decode('utf-8')}
            nxt = tf.next()
            x+=1
            if not x % 100000:
                t = time.time() - start
                xps = x/t
                ttls = ttl / xps
                print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")
                print(f"{done_x} with data / {x} = {done_x/x*100}%")
        self.out_cache.commit()
        tf.close()
