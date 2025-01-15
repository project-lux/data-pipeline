from .base import WdConfigManager
from pipeline.process.base.mapper import Mapper
from pipeline.process.utils.mapper_utils import make_datetime
from cromulent import model, vocab
from shapely.geometry import Polygon
import datetime
import math


class WdMapper(Mapper, WdConfigManager):
    def __init__(self, config):
        WdConfigManager.__init__(self, config)
        Mapper.__init__(self, config)
        self.acquirer = None
        self.precision_map = {11: "D", 12: "h", 13: "m", 14: "s", 10: "M", 9: "Y"}
        self.process_all_langs = False
        self.gender_map = {
            "Q6581072": vocab.instances["female"],
            "Q6581097": vocab.instances["male"],
            "Q2449503": vocab.instances["transgender"],  # trans male
            "Q1052281": vocab.instances["transgender"],  # trans female
            "Q1097630": vocab.instances["intersex"],
        }  # intersex --> aat:300438739

        self.ext_hash = {
            "P2163": "http://id.worldcat.org/fast/{ident}",
            "P1014": "http://vocab.getty.edu/aat/{ident}",
            "P245": "http://vocab.getty.edu/ulan/{ident}",
            "P1667": "http://vocab.getty.edu/tgn/{ident}",
            "P214": "https://viaf.org/viaf/{ident}",
            "P1566": "https://sws.geonames.org/{ident}",
            "P227": "https://d-nb.info/gnd/{ident}",
            "P4801": "http://id.loc.gov/vocabulary/{ident}",
            "P268": "http://data.bnf.fr/ark:/12148/cb{ident}",
            "P8516": "https://id.loc.gov/authorities/performanceMediums/{ident}",
            "P3763": "http://www.mimo-db.eu/InstrumentsKeywords/{ident}",
            "P846": "https://www.gbif.org/species/{ident}",
            "P227": "https://d-nb.info/gnd/{ident}",
            "P11858": "https://nsf.gov/awards/{ident}",
            "P3500": "https://ringgold.com/{ident}",
            "P6782": "https://ror.org/{ident}",
            "P496": "https://orcid.org/{ident}",
            "P3430": "https://snaccooperative.org/ark:/99166/{ident}",
        }

        self.nat_map = {
            "Q30": "http://vocab.getty.edu/aat/300107956",
            "Q142": "http://vocab.getty.edu/aat/300111188",
            "Q183": "http://vocab.getty.edu/aat/300111192",
            "Q17": "http://vocab.getty.edu/aat/300018519",
            "Q145": "http://vocab.getty.edu/aat/300111159",
            "Q29": "http://vocab.getty.edu/aat/300111215",
            "Q38": "http://vocab.getty.edu/aat/300111198",
            "Q15180": "http://vocab.getty.edu/aat/300111276",
            "Q9903": "http://vocab.getty.edu/aat/300018322",
            "Q36": "http://vocab.getty.edu/aat/300111204",
            "Q34": "http://vocab.getty.edu/aat/300111218",
            "Q16": "http://vocab.getty.edu/aat/300107962",
            "Q252": "http://vocab.getty.edu/aat/300018704",
            "Q29999": "http://vocab.getty.edu/aat/300111175",
            "Q155": "http://vocab.getty.edu/aat/300107967",
            "Q159": "http://vocab.getty.edu/aat/300111276",
            "Q174193": "http://vocab.getty.edu/aat/300111159",
            "Q668": "http://vocab.getty.edu/aat/300018863",
        } #covers ~3 mil out of 5.3 total across WD records in use

        #'P830': 'eol',
        #'P6944': 'bionomia',
        #'P213': 'isni',
        #'P8516': 'lcpm', # https://id.loc.gov/authorities/performanceMediums/{ident}
        #'P3763': 'mimo', # http://www.mimo-db.eu/InstrumentsKeywords/{ident}
        #'P402': 'osm', # Open Street Map relation id
        #'P349': 'ndl', # Japan
        #'P5587': 'snl' # Sweden

    def clean_date(self, date):
        if type(date) == list:
            date = date[0]
        if type(date) == dict:
            precision = date["precision"]
            date = date["time"]
        else:
            precision = 11

        if date[0] == "+":
            date = date[1:]
        if date[-1] == "Z":
            date = date[:-1]

        idx = date.find("-00")
        if idx > 0:  # Don't want to break -0010-01-00
            date = date[0] + date[1:].replace("-00", "-01")

        return (date, precision)

    def make_datetime(self, date, precision=11):
        if precision < 9:
            # 8 = decade, 7 = century, 6 = millenium
            # Log and ignore for now
            # print(f"Got precision {precision} for {date} -- ignoring")
            return None

        # Don't send extra data beyond precision
        if date[0] == "+":
            date = date[1:]
        if precision < 11:
            if date[0] == "-":
                (yy, mm, rest) = date[1:].split("-", 2)
                yy = "-" + yy
            else:
                (yy, mm, rest) = date.split("-", 2)
            if precision == 9:
                date = yy
            elif precision == 10:
                date = f"{yy}-{mm}"

        res = make_datetime(date, precision=self.precision_map[precision])
        if res:
            return res

    def guess_type(self, data):
        # using P31 is not possible to determine the class, as the class hierarchy
        # is self-contradictory in wikidata
        # So need to use a smell test by looking at properties

        # Concept: only care about common properties ... but need to know to build it
        #  ... best is to look for identifiers in known concept hierarchies like AAT
        #  ... P1014, P1843, P1036, P244 (value starting with sh)
        # Currency: P489, P562, P31:Q8142, P498,
        # Material: Also very hard to pick out. P31:Q214609, P2054, P2067
        # VisualItem:  Always merged with HMO? :(
        # Set: No equivalent? collection? Is conflated with organization
        # Digital: Never going to reconcile these

        # Situations when mismatch happens:
        # * Nationality -- wikidata has a Place, model expects a Type
        # * Organizations are often conflation of 2+ of Place, Group, Collection (Set), Building (HMO)

        # Some basic tests:
        if "identifier" in data and "data" in data:
            # Actually a record
            data = data["data"]

        useful_instance_of = {
            "Q4167410": None,  # Disambiguation page ... this isn't anything so abort
            "Q5": model.Person,  # Human = Person
            "Q4830453": model.Group,
            "Q43229": model.Group,
            "Q16334295": model.Group,
            "Q167037": model.Group,
            "Q783794": model.Group,
            "Q163740": model.Group,
            "Q34770": model.Language,
            "Q1288568": model.Language,
            "Q33742": model.Language,
            "Q20162172": model.Language,
            "Q436240": model.Language,
            "Q2315359": model.Language,
            "Q515": model.Place,
            "Q6256": model.Place,
            "Q3624078": model.Place,
            "Q7275": model.Place,
            "Q28575": model.Place,
            "Q82794": model.Place,
            "Q3957": model.Place,
            "Q1549591": model.Place,
            "Q702492": model.Place,
            "Q35657": model.Place,
            "Q106458883": model.Place,
            "Q34876": model.Place,
            "Q486972": model.Place,
            "Q15284": model.Place,
            "Q532": model.Place,
            "Q8502": model.Place,
            "Q484170": model.Place,
            "Q42744322": model.Place,
            "Q747074": model.Place,
            "Q208469": model.MeasurementUnit,
            "Q1978718": model.MeasurementUnit,
            "Q11344": model.Material,
            "Q1371562": model.MeasurementUnit,
            "Q1790144": model.MeasurementUnit,
            "Q3647172": model.MeasurementUnit,
            "Q3550873": model.MeasurementUnit,
            "Q12418": model.HumanMadeObject,
            "Q45585": model.HumanMadeObject,
            "Q175036": model.HumanMadeObject,
            "Q698487": model.HumanMadeObject,
            "Q464782": model.HumanMadeObject,
            "Q83872": model.HumanMadeObject,
            "Q1044742": model.HumanMadeObject,
            "Q1404472": model.Period,
            "Q45805": model.Period,
            "Q184963": model.Period,
            "Q11761": model.Period,
            "Q9903": model.Period,
            "Q173034": model.Activity,
            "Q901769": model.Activity,
            "Q688909": model.Activity,
            "Q193155": model.Activity,
            "Q459447": model.Activity
        }

        if "P31" in data:
            for p in data["P31"]:
                if p in useful_instance_of:
                    return useful_instance_of[p]

        prop_dist = {
            "person": [
                "P102",
                "P108",
                "P39",
                "P3368",
                "P69",
                "P21",
                "P569",
                "P570",
                "P19",
                "P20",
                "P734",
                "P735",
                "P106",
                "P1412",
                # P27 country of citizenship?
            ],
            "group": ["P112", "P740", "P159", "P488", "P749", "P2124", "P169", "P355", "P1037"],
            "place": [
                "P1281",
                "P190",
                "P7471",
                "P1937",
                "P1465",
                "P2326",
                "P473",
                "P1464",
                "P402",
                "P2046",
                "P1566",
                "P625",
                "P3896",
                "P2046",
                "P47",
                "P36",
                "P1082",
                "P6766",
                "P1566",
                "P1667",
                "P1332",
                "P1333",
                "P1334",
                "P1335",
            ],
            "activity": ["P580", "P582", "P710", "P1132", "P1542", "P664", "P585"],
            "period": ["P580", "P582", "P155", "P156", "P276"],
            "type": ["P1014", "P1843", "P1036"],
            "language": ["P282", "P1098", "P3823", "P218", "P219", "P220", "P1394"],
            "currency": ["P489", "P562", "P498"],
            "unit": ["P2370", "P2442", "P111"],
            "material": ["P2054", "P2067"],
            "object": ["P127", "P88", "P186", "P217", "P608", "P2049", "P176"],  # height used on people
            "text": ["P747", "P50", "P655", "P123", "P291", "P840"],
        }

        class_dist = {
            "person": model.Person,
            "group": model.Group,
            "place": model.Place,
            "type": model.Type,
            "language": model.Language,
            "currency": model.Currency,
            "unit": model.MeasurementUnit,
            "material": model.Material,
            "object": model.HumanMadeObject,
            "text": model.LinguisticObject,
            "activity": model.Activity,
            "period": model.Period
        }

        hits = {}
        for k, l in prop_dist.items():
            for p in l:
                if p in data:
                    try:
                        hits[k] += 1
                    except:
                        hits[k] = 1
        opts = list(hits.items())
        if opts:
            opts.sort(key=lambda x: x[1], reverse=True)
            typ = opts[0][0]
            return class_dist[typ]
        else:
            return class_dist["type"]

    def process_only_label(self, data, top):
        for lang in self.must_have:
            if lang in data["prefLabel"]:
                val = data["prefLabel"][lang]
                if val:
                    top._label = val
                    return
        for lang, val in data["prefLabel"].items():
            if lang in self.process_langs:
                top._label = val
                return

    def process_labels(self, data, top):
        # Top 25 (or so) languages spoken:
        # english (en), chinese (zh), hindi (hi), spanish (es), french (fr), arabic (ar)
        # german (de), bengali (bn), russian (ru), urdu (ur),
        # portuguese (pt), italian (it), greek (el), swahili (sw), japanese (ja),
        # indonesian (id), telugu (te), tamil (ta), turkish (tr), persian/farsi (fa),
        # korean (ko), thai (th), marathi (mr), punjabi (pa), dutch (nl), swedish (sv), finnish (fi)

        # Only preflabels - altlabels are full of garbage!

        vals = {}
        for lang in self.must_have:
            if lang in data["prefLabel"]:
                val = data["prefLabel"][lang]
                if not val in vals:
                    lbl = vocab.PrimaryName(content=val)
                    vals[val] = lbl
                    top.identified_by = lbl
                    if not hasattr(top, "_label"):
                        top._label = val
                else:
                    lbl = vals[val]
                lbl.language = self.process_langs[lang]

        # Might need to process everything to get any labels
        if self.process_all_langs or not hasattr(top, "identified_by"):
            for lang, val in data["prefLabel"].items():
                if lang in self.process_langs and not val in vals:
                    lbl = vocab.PrimaryName(content=val)
                    vals[val] = lbl
                    lbl.language = self.process_langs[lang]
                    top.identified_by = lbl
                    if not hasattr(top, "_label"):
                        top._label = val

        descs = []
        for lang in self.must_have:
            if lang in data["description"]:
                val = data["description"][lang]
                descs.append(val)
                desc = vocab.Description(content=val)
                desc.language = self.process_langs[lang]
                top.referred_to_by = desc

        if self.process_all_langs:
            for lang, val in data["description"].items():
                if lang in self.process_langs and not val in vals:
                    descs.append(val)
                    desc = vocab.Description(content=val)
                    desc.language = self.process_langs[lang]
                    top.referred_to_by = desc

    def process_equivalents(self, data, top):
        # Useful Equivalents
        sames = []
        for k, v in self.ext_hash.items():
            if k in data:
                for ki in data[k]:
                    sames.append(v.format(ident=ki))

        # P349 is both japan (ndlna) / japansh (ndlsh)
        # No way to distinguish, unlike LC below

        # Need to look at first character to determine which authority :(
        lc = data.get("P244", [])
        if lc:
            for x in lc:
                if x[0] == "s":
                    u = f"http://id.loc.gov/authorities/subjects/{x}"
                elif x[0] == "n":
                    u = f"http://id.loc.gov/authorities/names/{x}"
                else:
                    print(f"   --- unknown LC type: {x} in {data['id']}")
                    continue
                sames.append(u)

        # URGH
        # https://data.whosonfirst.org/890/424/287/890424287.geojson
        wofid = data.get("P6766", [])
        if wofid:
            for x in wofid:
                # f"https://data.whosonfirst.org/{x[:3]}/{x[3:6]}/.../{x}.geojson"
                chunks = []
                npid = x
                while npid:
                    if len(npid) > 3:
                        chunks.append(npid[:3])
                        npid = npid[3:]
                    else:
                        chunks.append(npid)
                        npid = ""
                uri = f"https://data.whosonfirst.org/{'/'.join(chunks)}/{x}.geojson"
                sames.append(uri)

        try:
            lbl = top._label
        except:
            lbl = "Equivalent Entity"
        for s in sames:
            top.equivalent = top.__class__(ident=s, label=lbl)

    def process_imageref(self, data, top, prop="P18"):
        image = data.get(prop, None)
        if image:
            # https://commons.wikimedia.org/wiki/File:Nadar_Niboyet.jpg
            for i in image:
                i = i.replace(" ", "_")
                ref = f"https://commons.wikimedia.org/wiki/Special:FilePath/{i}"
                jpg = vocab.DigitalImage()
                jpg.access_point = model.DigitalObject(ident=ref)
                img = model.VisualItem(label="Wikidata Image")
                img.digitally_shown_by = jpg
                top.representation = img

    def process_website(self, data, top):
        props = ["P856", "P973"]
        wp = False
        options = []
        for p in props:
            s = data.get(p, None)
            if s:
                if type(s) != list:
                    s = [s]
                for v in s:
                    if not v.endswith(".pdf"):
                        options.append(v)

        if "sitelinks" in data and "enwiki" in data["sitelinks"]:
            lnk = data["sitelinks"]["enwiki"]
            if "url" in lnk:
                options.append(lnk["url"])
            elif "title" in lnk:
                # construct it
                title = lnk["title"].strip().replace(" ", "_")
                url = f"https://en.wikipedia.org/wiki/{title}"
                options.append(url)

        if options:
            # already stripped pdfs, just take first for now
            url = options[0]
            lo = model.LinguisticObject(label="Website Text")
            do = vocab.WebPage(label="Home Page")
            do.access_point = model.DigitalObject(ident=url)
            lo.digitally_carried_by = do
            top.subject_of = lo

    def process_actor(self, data, top):
        # Relationship to Group: Member of P463, employer P108
        # member_of, unless Group is actually an Activity, then participant_in
        mbr = data.get("P463", None)
        if mbr:
            for grp in mbr:
                ref = self.get_reference(grp)
                if ref and ref.__class__ == model.Group:
                    top.member_of = model.Group(ident=self.expand_uri(grp))
                elif ref and ref.__class__ == model.Activity:
                    top.participated_in = model.Activity(ident=self.expand_uri(grp))

        mbr = data.get("P108", None)
        # only add employer if employer is Group
        if mbr:
            for grp in mbr:
                ref = self.get_reference(grp)
                if ref and ref.__class__ == model.Group:
                    top.member_of = ref

        participant = data.get("P1344", None)
        if participant:
            for p in participant:
                ref = self.get_reference(p)
                if ref and ref.__class__ == model.Activity:
                    top.participated_in = model.Activity(ident=self.expand_uri(p))
                    
        # FIXME: classified_as or professional activity as below
        occupation = data.get("P106", None)
        if occupation:
            for occ in occupation:
                top.classified_as = vocab.Occupation(ident=self.expand_uri(occ))

        #
        # Don't show residences
        #
        # residence = data.get("P551", None)
        # if residence:
        #    for rp in residence:
        #        top.residence = model.Place(ident=self.expand_uri(rp))

        #
        # Instead show occupation above
        #

        # field_of_work = data.get("P101", None)
        # work_location = data.get("P937", None)
        # work_start = data.get("P2031", None)
        # work_end = data.get("P2032", None)
        # if work_location or field_of_work or work_start or work_end:
        #     # Make a professional activities
        #     act = vocab.Active()
        #     if work_location:
        #         for wl in work_location:
        #             act.took_place_at = model.Place(ident=self.expand_uri(wl))
        #     if work_start or work_end:
        #         ts = model.TimeSpan()
        #         bstart = None
        #         dstart = None
        #         if work_start:
        #             bdate, precision = self.clean_date(work_start)
        #             try:
        #                 bstart, bend = self.make_datetime(bdate, precision)
        #             except TypeError:
        #                 bstart = None
        #             if bstart is not None:
        #                 ts.begin_of_the_begin = bstart
        #                 ts.end_of_the_begin = bend
        #         if work_end:
        #             ddate, precision = self.clean_date(work_end)
        #             try:
        #                 dstart, dend = self.make_datetime(ddate, precision)
        #             except TypeError:
        #                 dstart = None
        #             if dstart is not None:
        #                 ts.begin_of_the_end = dstart
        #                 ts.end_of_the_end = dend
        #         if bstart or dstart:
        #             act.timespan = ts
        #     if field_of_work:
        #         for fow in field_of_work:
        #             act.classified_as = model.Type(ident=self.expand_uri(fow))

    def process_person(self, data, top):
        # Name Parts: Family Name P734, Given Name P735
        # SKIP -- Don't need these in LUX
        # Family: Father P22, Mother P25
        # SKIP -- No good modeling for parents

        gender = data.get("P21", None)
        if not type(gender) == list:
            gender = [gender]
        if gender:
            for g in gender:
                if g in self.gender_map:
                    top.classified_as = self.gender_map[g]
                else:
                    # FIXME: logging
                    # print(f"Unknown wikidata gender: {g}")
                    pass

        bdate = data.get("P569", None)
        if bdate:
            bdate, precision = self.clean_date(bdate)
            try:
                bstart, bend = self.make_datetime(bdate, precision)
            except TypeError:
                bstart = None
            if bstart is not None:
                birth = model.Birth()
                ts = model.TimeSpan()
                birth.timespan = ts
                ts.begin_of_the_begin = bstart
                ts.end_of_the_end = bend
                # FIXME: This should take into account precision
                # and leading - for BCE
                ts.identified_by = vocab.DisplayName(content=bdate[:10])
                top.born = birth

        bplace = data.get("P19", None)
        if bplace:
            if type(bplace) == list:
                # Can only be born in one place :P
                bplace = bplace[0]
            if not hasattr(top, "born"):
                birth = model.Birth()
                top.born = birth
            else:
                birth = top.born
            birth.took_place_at = model.Place(ident=self.expand_uri(bplace))

        ddate = data.get("P570", None)
        if ddate:
            ddate, precision = self.clean_date(ddate)
            try:
                dstart, dend = self.make_datetime(ddate, precision)
            except TypeError:
                dstart = None
            if dstart is not None:
                death = model.Death()
                ts = model.TimeSpan()
                death.timespan = ts
                ts.begin_of_the_begin = dstart
                ts.end_of_the_end = dend
                ts.identified_by = vocab.DisplayName(content=ddate[:10])
                top.died = death

        dplace = data.get("P20", None)
        if dplace:
            if type(dplace) == list:
                # Can only die in one place :P
                dplace = dplace[0]
            if not hasattr(top, "died"):
                death = model.Death()
                top.died = death
            else:
                death = top.died
            death.took_place_at = model.Place(ident=self.expand_uri(dplace))

        dtype = data.get("P1196", None)
        if dtype:
            # e.g. natural causes vs suicide
            if not hasattr(top, "died"):
                death = model.Death()
                top.died = death
            else:
                death = top.died
            if not type(dtype) == list:
                dtype = [dtype]
            for dt in dtype:
                death.classified_as = model.Type(ident=self.expand_uri(dt))

        nationality = data.get("P27", None)
        if nationality:
            for n in nationality:            
                if n in self.nat_map:
                    top.classified_as = vocab.Nationality(ident=self.nat_map[n])

        # FIXME: Need to map country of origin to nationality as classification
        # Reconcile Place.P1549 with AAT?
        # 1549 is just a lang-string :(
        # SELECT ?item ?itemLabel ?demo
        # WHERE
        # {
        #  ?item wdt:P1549 ?demo ; wdt:P31 wd:Q6256 . FILTER(lang(?demo)='en')
        #  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". } # Helps get the label in your language, if not, then en language
        # }
        # But there's Q33829 Human Population, which has P27 Country of Citizenship
        # Might be able to back into it comparing P1549 and P27 ?

        # Relationship to Group: Ethnic Group P172, Religion P140
        # classifications? memberships?

        # Process shared actor field
        self.process_actor(data, top)
        self.process_imageref(data, top)

    def process_group(self, data, top):
        # ["P112", "P159", "P488", "P749", "P2124", "P169", "P355", "P1037"],

        parent = data.get("P749", None)
        if parent:
            for p in parent:
                top.member_of = model.Group(ident=self.expand_uri(p))

        bdate = data.get("P571", None)
        if bdate:
            bdate, precision = self.clean_date(bdate)
            try:
                bstart, bend = self.make_datetime(bdate, precision)
            except TypeError:
                bstart = None
            if bstart is not None:
                birth = model.Formation()
                ts = model.TimeSpan()
                birth.timespan = ts
                ts.begin_of_the_begin = bstart
                ts.end_of_the_end = bend
                ts.identified_by = vocab.DisplayName(content=bdate[:10])
                top.formed_by = birth

        bplace = data.get("P740", None)
        if bplace:
            if type(bplace) == list:
                # Can only be born in one place :P
                bplace = bplace[0]
            if not hasattr(top, "formed_by"):
                birth = model.Formation()
                top.formed_by = birth
            else:
                birth = top.formed_by
            birth.took_place_at = model.Place(ident=self.expand_uri(bplace))

        founder = data.get("P112", None)
        if founder:
            if not hasattr(top, "formed_by"):
                birth = model.Formation()
                top.formed_by = birth
            else:
                birth = top.formed_by
            for f in founder:
                ref = self.get_reference(f)
                if ref and ref.__class__ in [model.Person, model.Group]:
                    birth.carried_out_by = ref

        ddate = data.get("P576", None)
        if ddate:
            ddate, precision = self.clean_date(ddate)
            try:
                dstart, dend = self.make_datetime(ddate, precision)
            except TypeError:
                dstart = None
            if dstart is not None:
                death = model.Dissolution()
                ts = model.TimeSpan()
                death.timespan = ts
                ts.begin_of_the_begin = dstart
                ts.end_of_the_end = dend
                ts.identified_by = vocab.DisplayName(content=ddate[:10])
                top.dissolved_by = death

        # headquarters
        # residence = data.get("P159", None)
        # if residence:
        #    for rp in residence:
        #        top.residence = model.Place(ident=self.expand_uri(rp))

        # street address
        saddr = data.get("P6375", None)
        if saddr:
            saddr = saddr[0]
            if type(saddr) == dict:
                # {'en': '...'}
                saddr = list(saddr.values())[0]
            top.contact_point = vocab.StreetAddress(content=saddr)

        self.process_actor(data, top)
        self.process_imageref(data, top)
        self.process_imageref(data, top, prop="P154")

    def process_place(self, data, top):
        # Need to filter types down, so do pre-re-identification
        place_type_map = {
            "Q5107": "300128176",  # Continent
            "Q6256": "300128207",  # Country
            "Q35657": "300000776",  # US State -> Province / State
            "Q106458883": "300000776",  # State
            "Q515": "300008389",  # City
        }

        # regions are divided up per country in wikidata
        # So can't do an easy test for county / provice / region etc. :(
        # Classifications
        # P31: Q6256 = Country
        types = data.get("P31", [])
        for q, aat in place_type_map.items():
            if q in types:
                top.classified_as = model.Type(ident=f"http://vocab.getty.edu/aat/{aat}")
                break

        # Part of + located in the administrative territorial entity
        broader = data.get("P361", [])
        broader.extend(data.get("P131", []))
        # otherwise country
        if not broader:
            broader = data.get("P17", [])

        for b in broader:
            ref = self.get_reference(b)
            if ref and ref.__class__ == model.Place:
                top.part_of = model.Place(ident=self.expand_uri(b))
            

        # Coordinates
        northmost = data.get("P1332", None)
        southmost = data.get("P1333", None)
        eastmost = data.get("P1334", None)
        westmost = data.get("P1335", None)

        coords = []
        if northmost and southmost and eastmost and westmost:
            # This is a polygon

            coords = []
            for pt in [northmost, eastmost, southmost, westmost]:
                if type(pt) == list:
                    pt = pt[0]
                pt = [float(pt["long"]), float(pt["lat"])]
                coords.append(pt)

            # make into a box
            nw = [coords[3][0], coords[0][1]]
            ne = [coords[1][0], coords[0][1]]
            se = [coords[1][0], coords[2][1]]
            sw = [coords[3][0], coords[2][1]]

            old = coords
            coords = [nw, ne, se, sw, nw]
            p = Polygon(coords)
            if p.area > 2000:
                coords = []

            if coords:
                pairs = [f"{p[0]} {p[1]}" for p in coords]
                top.defined_by = f"POLYGON (( {','.join(pairs)} ))"

        if not coords:
            coords = data.get("P625", None)
            if coords:
                # This is a point

                # Rob trusts WD > TGN
                if type(coords) == list:
                    # Pick first one, essentially at random
                    coords = coords[0]
                # dict of lat, long, alt
                if type(coords) == dict:
                    coords = [float(coords["long"]), float(coords["lat"])]

                top.defined_by = f"POINT ( {coords[0]} {coords[1]} )"

        # UI doesn't need images on places
        # and search ends up searching the attribution, which makes things weird
        # self.process_imageref(data, top)
        # self.process_imageref(data, top, prop="P1943")

    def process_type(self, data, top):
        return self.process_concept(data, top)

    def process_concept(self, data, top):
        # This is basically impossible without more explicit knowledge
        # P737 --> influenced by (meaning its creation)

        # codes for Materials
        elm = data.get("P246", None)
        if elm:
            elm = elm[0]
            top.identified_by = model.Identifier(content=elm)
        else:
            elm = data.get("P274")
            if elm:
                elm = elm[0]
                top.identified_by = model.Identifier(content=elm)

        # codes for Languages
        two = data.get("P218", None)
        if two:
            if type(two) == list:
                two = two[0]
            top.identified_by = model.Identifier(content=two)

        three = data.get("P219", None)
        if three:
            if type(three) == list:
                three = three[0]
            top.identified_by = model.Identifier(content=three)
        else:
            threeb = data.get("P220", None)
            if threeb:
                if type(threeb) == list:
                    threeb = threeb[0]
                top.identified_by = model.Identifier(content=threeb)

        # codes for currencies
        # FIXME

    def process_language(self, data, top):
        self.process_concept(data, top)

    def process_material(self, data, top):
        # materials probably safe to process images for
        self.process_imageref(data, top)
        self.process_concept(data, top)

    def process_currency(self, data, top):
        # start and end time?
        self.process_imageref(data, top)
        self.process_concept(data, top)

    def process_unit(self, data, top):
        self.process_concept(data, top)

    def process_measurementunit(self, data, top):
        self.process_concept(data, top)

    # Core record types

    def process_humanmadeobject(self, data, top):
        # Find common P31 links
        # Q3305213 --> painting
        # Q860861 --> sculpture
        type_map = {
            "Q3305213": "300033618",  # Painting
            "Q860861": "300047090",  # Sculpture
            "Q93184": "300033973",  # Drawing
            "Q125191": "300046300",  # Photograph
        }
        classns = data.get("P31", [])
        for k, v in type_map.items():
            if k in classns:
                top.classified_as = model.Type(ident=f"http://vocab.getty.edu/aat/{v}")
                break

        # Production
        bdate = data.get("P571", None)
        # production date
        creator = data.get("P170", None)
        if creator is None:
            # try manufacturer
            creator = data.get("P176", None)
        # production carried_out_by
        prod_loc = data.get("P1071", None)
        # production took_place_at
        # commiss = data.get("P88", None)
        # production commissioned by --> don't have this data anywhere else
        # And requires full provenance activity modeling:
        # https://linked.art/model/provenance/promises/#commissions-for-artwork

        if bdate or creator or prod_loc:
            bstart = None
            prod = model.Production()
            if bdate:
                bdate, precision = self.clean_date(bdate)
                try:
                    bstart, bend = self.make_datetime(bdate, precision)
                except TypeError:
                    bstart = None
                if bstart is not None:
                    ts = model.TimeSpan()
                    prod.timespan = ts
                    ts.begin_of_the_begin = bstart
                    ts.end_of_the_end = bend
            if creator:
                # Could be a Group
                for c in creator:
                    ref = self.get_reference(c)
                    if ref and ref.__class__ in [model.Person, model.Group]:
                        prod.carried_out_by = ref
            if prod_loc:
                for p in prod_loc:
                    prod.took_place_at = model.Place(ident=self.expand_uri(p))
            if bstart or creator or prod_loc:
                top.produced_by = prod

        # curloc = data.get("P276", None) --> collection
        # loc = data.get("P625", None) # coordinate loc is real place
        # ... but just coordinates :(
        # current location ... but might not be a place
        # e.g. a museum = in collection of museum
        # in_coll = data.get("P195", None)
        # collection ... same as location!
        # so can't just use it directly as could be place, set or group :(
        # cur_owner = data.get('P127', None)
        # current owner ... same issue as location, collection

        catcode = data.get("P528", None)
        # identifier in some catalog
        if catcode:
            for cc in catcode:
                top.identified_by = model.Identifier(content=cc)

        invnumb = data.get("P217", None)
        # accession number
        if invnumb:
            for inv in invnumb:
                top.identified_by = vocab.AccessionNumber(content=inv)

        mats = data.get("P186", [])
        # materials
        if mats:
            for m in mats:
                top.made_of = model.Material(ident=self.expand_uri(m))

        width = data.get("P2049", None)
        height = data.get("P2048", None)
        depth = data.get("P2610", None)
        # dimensions
        for d, dt in [(width, vocab.Width), (height, vocab.Height), (depth, vocab.Depth)]:
            if d:
                d = d[0]
                val = d[0]
                if val[0] == "+":
                    val = float(val[1:])
                else:
                    val = float(val)
                unit = d[1]
                # map common units
                if unit.endswith("Q174728"):
                    unit = vocab.instances["cm"].id
                elif unit.endswith("Q218593"):
                    unit = vocab.instances["inches"].id
                else:
                    unit = self.expand_uri(unit)
                dim = dt(value=val)
                dim.unit = model.MeasurementUnit(ident=unit)
                top.dimension = dim

        enc_loc = data.get("P189", None)
        enc_date = data.get("P575", None)
        if enc_loc or enc_date:
            enc = model.Encounter()
        if enc_loc:
            for el in enc_loc:
                enc.took_place_at = model.Place(ident=self.expand_uri(el))
        if enc_date:
            enc_date, precision = self.clean_date(enc_date)
            try:
                bstart, bend = self.make_datetime(enc_date, precision)
            except TypeError:
                bstart = None
            if bstart is not None:
                ts = model.TimeSpan()
                ts.begin_of_the_begin = bstart
                ts.end_of_the_end = bend
                enc.timespan = ts
        if enc_loc or enc_date:
            top.encountered_by = enc

        if 0:
            # FIXME: This causes a mess
            genre = data.get("P136", None)
            movement = data.get("P135", None)
            depicts = data.get("P180", None)
            subj = data.get("P921", None)
            if genre or movement or depicts or subj:
                viqid = self.configs.make_qua(top.id, "VisualItem")
                vi = model.VisualItem(ident=viqid)
                top.shows = vi

        self.process_imageref(data, top)

    def process_work(self, data, top):
        genre = data.get("P136", None)
        if genre:
            for g in genre:
                top.classified_as = model.Type(ident=self.expand_uri(g))
        subj = data.get("P921", None)
        if subj:
            for s in subj:
                ref = self.get_reference(s)
                if ref:
                    top.about = ref
        mvmt = data.get("P135", None)
        if mvmt:
            for m in mvmt:
                top.classified_as = model.Type(ident=self.expand_uri(m))

        author = data.get("P50", [])
        cre_place = data.get("P495", [])
        cre_date = data.get("P571", [])
        if author or cre_place or cre_date:
            cre = model.Creation()
            top.created_by = cre
            if author:
                for a in author:
                    ref = self.get_reference(a)
                    if ref and ref.__class__ in [model.Person, model.Group]:
                        cre.carried_out_by = ref
            if cre_place:
                for p in cre_place:
                    cre.took_place_at = model.Place(ident=self.expand_uri(p))
            if cre_date:
                bdate, precision = self.clean_date(cre_date)
                try:
                    bstart, bend = self.make_datetime(bdate, precision)
                except TypeError:
                    bstart = None
                if bstart is not None:
                    ts = model.TimeSpan()
                    cre.timespan = ts
                    ts.begin_of_the_begin = bstart
                    ts.end_of_the_end = bend
                    ts.identified_by = vocab.DisplayName(content=bdate[:10])

        publisher = data.get("P123", [])
        pub_place = data.get("P291", [])
        pub_date = data.get("P577", None)
        if publisher or pub_place or pub_date:
            pub = vocab.Publishing()
            top.used_for = pub
            if publisher:
                for p in publisher:
                    ref = self.get_reference(p)
                    if ref and ref.__class__ in [model.Group, model.Person]:
                        pub.carried_out_by = ref
            if pub_place:
                for p in pub_place:
                    pub.took_place_at = model.Place(ident=self.expand_uri(p))
            if pub_date:
                bdate, precision = self.clean_date(pub_date)
                try:
                    bstart, bend = self.make_datetime(bdate, precision)
                except TypeError:
                    bstart = None
                if bstart is not None:
                    ts = model.TimeSpan()
                    pub.timespan = ts
                    ts.begin_of_the_begin = bstart
                    ts.end_of_the_end = bend
                    ts.identified_by = vocab.DisplayName(content=bdate[:10])

    def process_visualitem(self, data, top):
        depicts = data.get("P180", None)
        if depicts:
            for d in depicts:
                ref = self.get_reference(d)
                if ref:
                    top.represents = ref
        self.process_work(data, top)
        self.process_imageref(data, top)

    def process_linguisticobject(self, data, top):
        lang = data.get("P407", [])
        if lang:
            for l in lang:
                top.language = model.Language(ident=self.expand_uri(l))

        self.process_work(data, top)
        self.process_imageref(data, top)

    def process_event(self, data, top):
        self.process_imageref(data, top)

    def process_period(self, data, top):
        self.process_imageref(data, top)
        self.process_activity(data, top)

    def process_activity(self, data, top):
        startTime = data.get("P580")
        endTime = data.get("P582")
        ts = model.TimeSpan()
        
        if startTime:
            startTime, precision = self.clean_date(startTime)
            try:
                bstart, bend = self.make_datetime(startTime, precision)
                ts.begin_of_the_begin = bstart
                ts.end_of_the_begin = bend
            except TypeError:
                pass
        
        if endTime:
            endTime, precision = self.clean_date(endTime)
            try:
                estart, eend = self.make_datetime(endTime, precision)
                ts.begin_of_the_end = estart
                ts.end_of_the_end = eend
            except TypeError:
                pass
        
        if startTime or endTime:
            top.timespan = ts
                
        participant = data.get("P710",[])
        chairperson = data.get("P488",[])
        participants = participant + chairperson
        if participants:
            for p in participants:
                pref = self.get_reference(p)
                if pref and pref.__class__ in [model.Group, model.Person]:
                    top.participant = pref

        #P361 part_of
        broader = data.get("P361",[])
        if broader:
            for b in broader:
                pref = self.get_reference(b)
                if pref and pref.__class__ == model.Period:
                    top.part_of = model.Period(ident=self.expand_uri(b))
                elif pref and pref.__class__ == model.Activity:
                    top.part_of = model.Activity(ident=self.expand_uri(b))


        country = data.get("P17",[])
        location = data.get("P276",[])
        venue = data.get("P2293",[])
        
        places = country + location + venue
        if places:
            for p in places:
                top.took_place_at = model.Place(ident=self.expand_uri(p))

        self.process_imageref(data, top)

    def process_digitalobject(self, data, top):
        # FIXME: Not sure what to do with this, or what would cause it?
        print(f"WD Mapper found a top level digital object")
        self.process_linguisticobject(data, top)

    def process_set(self, data, top):
        print(f"WD Mapper was given a Set")

    def transform(self, record, rectype=None, reference=False):
        # Take native wikidata json and produce linked art
        # self.factory is a pre-configured factory...
        # ... but it is just the same factory instance from model
        if not self.acquirer:
            self.acquirer = self.config["acquirer"]

        data = record["data"]
        myid = f"{self.namespace}{data['id']}"

        if not rectype:
            crmcls = self.guess_type(data)
            if not crmcls:
                return None
            rectype = crmcls.__name__
        else:
            crmcls = getattr(model, rectype)
        top = crmcls(ident=myid)

        if reference:
            self.process_only_label(data, top)
        else:
            # Do common properties
            self.process_labels(data, top)
            self.process_equivalents(data, top)
            self.process_website(data, top)
            typ = rectype.lower()
            fn = getattr(self, f"process_{typ}")
            fn(data, top)

        data = model.factory.toJSON(top)
        return {"data": data, "identifier": record["identifier"], "source": "wikidata"}
