
from pipeline.process.base.mapper import Mapper
from cromulent import model, vocab
from bs4 import BeautifulSoup
import ujson as json

class WmMapper(Mapper):

    def __init__(self, config):
        Mapper.__init__(self, config)
        self.licenses = config.get('allowed_licenses', ['pd', 'cc0', 'cc-by-sa-4.0', 'cc-by-4.0', 'cc-by-sa-3.0', 'cc-by-3.0'])

    def transform(self, record, rectype, reference=False):

        # take a image info API response from wikimedia commons
        # if good: 
        #   return a DigitalObject suitable to be the object of digitally_shown_by
        # if not exists: return None
        # if not good license: return None   (?)

        try:
            info = record['data']['query']['pages'].popitem()[1]
        except:
            # Missing image
            #print(f"no data: {record['identifier']}")
            return None
        try:
            imginfo = info['imageinfo'][0]['extmetadata']
        except:
            #print(f"no image: {record['identifier']}")
            return None
        lic = imginfo.get('License', {'value':''})['value']
        if not lic or not lic in self.licenses:
            #print(f"bad license: {lic} for {record['identifier']}")
            return None

        # We're okay to use

        do = vocab.DigitalImage()
        title = info['title']
        if title.startswith('File:'):
            title = title[5:]
        tl = title.lower()
        if not tl.endswith('.jpg') and not tl.endswith('.jpeg') and not tl.endswith('.gif') and not tl.endswith('.png'):
            return None
        uri = f"https://commons.wikimedia.org/wiki/Special:Filepath/{title}"
        do.access_point = model.DigitalObject(ident=uri)

        name = imginfo.get('ObjectName', {'value':''})['value']
        soup = BeautifulSoup(name, 'lxml')
        cln_name = soup.get_text()
        cln_name = cln_name.replace('\n', ' ').strip()
        if cln_name != "":
            do.identified_by = vocab.PrimaryName(value=cln_name)

        desc = imginfo.get('ImageDescription', {'value':''})['value']
        if desc:
            soup = BeautifulSoup(desc, 'lxml')
            cln_desc = soup.get_text()
            cln_desc = cln_desc.replace('\n', ' ').strip()
            if cln_desc:
                do.referred_to_by = vocab.Description(value=cln_desc)

        licurl = imginfo.get('LicenseUrl', {'value':''})['value']
        licname = imginfo.get('UsageTerms', {'value':''})['value']
        if licurl:
            # Go the extra mile and assert a Right
            rt = model.Right()
            rturi = model.Type(ident=licurl)
            rt.classified_as = rturi
            rt.identified_by = model.Name(content=licname)
            do.subject_to = rt

        licsn = imginfo.get('LicenseShortName', {'value':''})['value']
        credit = imginfo.get('Credit', {'value':''})['value']
        artist = imginfo.get('Artist', {'value':''})['value']

        # (name|title) [by artist] [credit], lic
        cname = cln_name if cln_name else title
        c = f" {credit}" if credit else ""
        a = f" by {artist}" if artist else ""
        if licurl:            
            ltxt = f'<a href="{licurl}">{licsn}</a>'
        else:
            ltxt = licname

        credit = f"<span>{cname}{a}{c}, {ltxt}</span>"
        do.referred_to_by = vocab.RightsStatement(content=credit)

        js = model.factory.toJSON(do)
        return {'identifier': record['identifier'], 'data': js, 'source': self.name}

