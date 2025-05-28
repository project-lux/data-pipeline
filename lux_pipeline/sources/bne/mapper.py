from lux_pipeline.process.base.mapper import Mapper
from lux_pipeline.process.utils.date_utils import test_birth_death, make_datetime
from cromulent import model, vocab
import logging

logger = logging.getLogger("lux_pipeline")

# sparql endpoint: https://datos.bne.es/sparql
# ontology: https://datos.bne.es/def/index-es.html
# mapping: concepts, people


class BneMapper(Mapper):
    def __init__(self, config):
        Mapper.__init__(self, config)

    def guess_type(self, data):
        typ_uri = data.get("@type", "")
        scheme = data.get("inScheme", "")
        if scheme == "https://datos.bne.es/def/geograficos":
            topcls = model.Place
        elif typ_uri == "http://www.w3.org/2004/02/skos/core#Concept":
            topcls = model.Type
        elif typ_uri == "https://datos.bne.es/def/C1005":
            topcls = model.Person
        elif typ_uri == "https://datos.bne.es/def/C1006":
            # Entidad Corporativa (per https://datos.bne.es/def/index-es.html#C1006)
            topcls = model.Group
        else:
            # unhandled type
            topcls = None
        return topcls

    def handle_common(self, rec, top, topcls):
        preflist = rec.get("prefLabel", [])
        dupes = {}
        if type(preflist) != list:
            preflist = [preflist]
        for item in preflist:
            preflbl = item.get("@value", "")
            preflang = item.get("@language", "")
            dupes = {preflbl: 1}
            if preflbl:
                nm = vocab.PrimaryName(content=preflbl)
                lang = self.process_langs.get(preflang, None)
                if lang:
                    nm.language = lang
                top.identified_by = nm
            else:
                logger.error(f"No preflabel in {rec['id']}")

        altlbls = rec.get("altLabel", [])
        if type(altlbls) != list:
            altlbls = [altlbls]
        for a in altlbls:
            if type(a) == dict:
                altlbl = a.get("@value", "")
                altlang = a.get("@language", "")
            elif type(a) == str:
                altlbl = a
                altlang = None
            if altlbl and not altlbl in dupes:
                dupes[altlbl] = 1
                an = vocab.AlternateName(content=altlbl)
                if altlang:
                    lang = self.process_langs.get(altlang, None)
                    if lang:
                        an.language = lang
                top.identified_by = an

        sameAs = rec.get("sameAs", [])
        if type(sameAs) != list:
            sameAs = [sameAs]
        for same in sameAs:
            top.equivalent = topcls(ident=same)

        close = rec.get("closeMatch", [])
        if type(close) != list:
            close = [close]
        for c in close:
            if c:
                top.equivalent = topcls(ident=c)

    def handle_place(self, rec, top, topcls):
        self.handle_common(rec, top, topcls)
        broader = rec.get("broader", [])
        if type(broader) != list:
            broader = [broader]
        for b in broader:
            top.part_of = topcls(ident=b)

        # "lat": "N0354107", = 35o41'07" ?
        # "long": "E1394511", = 139o45'11" ?
        lat = rec.get("lat", "")
        lng = rec.get("long", "")
        if lat and lng:
            # top.defined_by = f"POINT ({lng} {lat})"
            pass

    def transform(self, record, rectype="", reference=False):
        try:
            rec = record["data"]["@graph"][0]
        except:
            logger.debug(f"BNE record {record['identifier']} doesn't have @graph")
            return None
        if rectype:
            topcls = getattr(model, rectype)
        else:
            topcls = self.guess_type(rec)

        if topcls:
            rectype = topcls.__name__
        else:
            return None

        top = topcls(ident=rec["@id"])
        # Per class specific stuff
        fn = getattr(self, f"handle_{rectype.lower()}", None)
        if fn:
            fn(rec, top, topcls)

        data = model.factory.toJSON(top)
        return {"identifier": record["identifier"], "data": data, "source": "bne"}

    def handle_type(self, rec, top, topcls):
        self.handle_common(rec, top, topcls)
        broader = rec.get("broader", [])
        if type(broader) != list:
            broader = [broader]
        for b in broader:
            top.broader = topcls(ident=b)

    def handle_person(self, rec, top, topcls):
        prefname = rec.get("P5001", "")
        dupes = {prefname: 1}
        top.identified_by = vocab.PrimaryName(content=prefname)

        altlbls = rec.get("P5012", [])
        if type(altlbls) != list:
            altlbls = [altlbls]
        for alt in altlbls:
            if not alt in dupes:
                dupes[alt] = 1
                top.identified_by = vocab.AlternateName(content=alt)

        p5024 = rec.get("P5024", [])
        seeAlso = rec.get("seeAlso", "")
        sameAs = rec.get("sameAs", [])
        if type(p5024) != list:
            p5024 = [p5024]
        if type(sameAs) != list:
            sameAs = [sameAs]
        sames = [x for x in p5024 if x not in sameAs] + sameAs
        if type(seeAlso) != list:
            seeAlso = [seeAlso]
        sames = [x for x in sames if x not in seeAlso] + seeAlso
        for same in sames:
            if same:
                if type(same) in [dict, list]:
                    logger.warning(f"Expected string in BNE person same-as, got {same}")
                else:
                    top.equivalent = topcls(ident=same)

        dob = rec.get("P5010", "")
        pob = rec.get("P50119", "")
        d = None
        if dob:
            ts = model.TimeSpan()
            birth = model.Birth()
            if type(dob) == list and len(dob) == 2:
                b = dob[0]
                d = dob[1]
                begins = make_datetime(b)
            else:
                begins = make_datetime(dob)
            if begins:
                ts.begin_of_the_begin = begins[0]
                ts.end_of_the_end = begins[1]
            ts.identified_by = vocab.DisplayName(content=dob)
            if pob:
                ts.took_place_at = model.Place(label=pob)
            birth.timespan = ts
            top.born = birth

        dod = rec.get("P5011", "")
        pod = rec.get("P50118", "")
        ends = None
        if dod:
            ends = make_datetime(dod)
        elif d:
            ends = make_datetime(d)
        if ends:
            ts = model.TimeSpan()
            death = model.Death()
            if ends:
                ts.begin_of_the_begin = ends[0]
                ts.end_of_the_end = ends[1]
            ts.identified_by = vocab.DisplayName(content=dod)
            if pod:
                ts.took_place_at = model.Place(label=pod)
            death.timespan = ts
            top.died = death

        gender = rec.get("P50116", "")
        if gender:
            if gender == "Masculino":
                top.classified_as = vocab.instances["male"]
            elif gender == "Femenino":
                top.classified_as = vocab.instances["female"]
            else:
                top.classified_as = vocab.Gender(ident=gender)

        depiction = rec.get("P3066", "")
        if type(depiction) == list:
            depiction = depiction[0]
        if depiction:
            image = model.VisualItem()
            top.representation = image
            dig_img = vocab.DigitalImage()
            dig_img.access_point = model.DigitalObject(ident=depiction)
            image.digitally_shown_by = dig_img

        bio = rec.get("P3067", "")
        if bio:
            biost = vocab.BiographyStatement(content=bio)
            biost.language = vocab.instances["spanish"]
            top.referred_to_by = biost

        # unreconciled
        nationality = rec.get("P50102", "")
        if nationality:
            top.classified_as = vocab.Nationality(label=nationality)

        classification = rec.get("P50104", [])
        if type(classification) != list:
            classification = [classification]
        for cxn in classification:
            class_as = model.Type(label=cxn)
            class_as.language = vocab.instances["spanish"]
            top.classified_as = class_as

        okay = test_birth_death(top)
        if not okay:
            try:
                top.born = None
                top.died = None
            except:
                # This shouldn't ever happen, but not going to die on the hill
                pass
