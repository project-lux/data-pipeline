
from html import escape

from lux_pipeline.process.base.mapper import Mapper
from lux_pipeline.sources.wikimedia import wikitext
from cromulent import model, vocab
from bs4 import BeautifulSoup
import ujson as json

class WmMapper(Mapper):
    """A Commons image as the DigitalImage a record's representation is
    digitally shown by, when its licence is in ``allowed_licenses``. Reads
    both shapes the datacache holds: the imageinfo API response the
    fetcher stores, and a file page from a dump (`WmLoader`: the page's
    wikitext with its ``license``)."""

    def __init__(self, config):
        Mapper.__init__(self, config)
        self.licenses = config.get('allowed_licenses', ['pd', 'cc0', 'cc-by-sa-4.0', 'cc-by-4.0'])

    def api_metadata(self, data):
        # take a image info API response from wikimedia commons
        try:
            info = next(iter(data['query']['pages'].values()))
        except:
            # Missing image
            return None
        try:
            imginfo = info['imageinfo'][0]['extmetadata']
        except:
            return None
        value = lambda k: imginfo.get(k, {'value': ''})['value']

        name = BeautifulSoup(value('ObjectName'), 'lxml').get_text()
        desc = value('ImageDescription')
        if desc:
            desc = BeautifulSoup(desc, 'lxml').get_text()
        return {"title": info['title'], "license": value('License'),
                "name": name.replace('\n', ' ').strip(), "description": desc.replace('\n', ' ').strip(),
                "licurl": value('LicenseUrl'), "licname": value('UsageTerms'),
                "licsn": value('LicenseShortName'), "credit": value('Credit'), "artist": value('Artist')}

    def dump_metadata(self, data):
        # a file page from a dump: the licence the loader found, the rest
        # from the page's Information template
        lic = data.get('license') or ''
        terms = wikitext.LICENSES.get(lic, {})
        fields = wikitext.file_fields(data.get('wikitext', ''))
        title = data.get('title', '')
        # as the API's ObjectName: the file name without its extension
        name = fields.get('title') or title.split(':', 1)[-1].rsplit('.', 1)[0]
        return {"title": title, "license": lic,
                "name": name, "description": fields.get('description', ''),
                "licurl": terms.get('url', ''), "licname": terms.get('terms', ''),
                "licsn": terms.get('short', ''), "credit": escape(fields.get('source', '')),
                "artist": escape(fields.get('author', ''))}

    def transform(self, record, rectype, reference=False):

        # if good:
        #   return a DigitalObject suitable to be the object of digitally_shown_by
        # if not exists: return None
        # if not good license: return None   (?)

        data = record.get('data')
        if not isinstance(data, dict):
            return None
        meta = self.dump_metadata(data) if 'wikitext' in data else self.api_metadata(data)
        if meta is None:
            return None
        lic = meta['license']
        if not lic or not lic in self.licenses:
            return None

        # We're okay to use

        do = vocab.DigitalImage()
        title = meta['title']
        if title.startswith('File:'):
            title = title[5:]
        tl = title.lower()
        if not tl.endswith('.jpg') and not tl.endswith('.jpeg') and not tl.endswith('.gif') and not tl.endswith('.png'):
            return None
        uri = f"https://commons.wikimedia.org/wiki/Special:Filepath/{title}"
        do.access_point = model.DigitalObject(ident=uri)

        cln_name = meta['name']
        if cln_name != "":
            do.identified_by = vocab.PrimaryName(value=cln_name)

        if meta['description']:
            do.referred_to_by = vocab.Description(value=meta['description'])

        licurl = meta['licurl']
        licname = meta['licname']
        if licurl:
            # Go the extra mile and assert a Right
            rt = model.Right()
            rturi = model.Type(ident=licurl)
            rt.classified_as = rturi
            rt.identified_by = model.Name(content=licname)
            do.subject_to = rt

        licsn = meta['licsn']
        credit = meta['credit']
        artist = meta['artist']

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
