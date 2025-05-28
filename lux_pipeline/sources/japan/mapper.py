from lux_pipeline.process.base.mapper import Mapper
from cromulent import model, vocab
from lux_pipeline.process.utils.date_utils import make_datetime


class GenericJapanMapper(Mapper):
    def __init__(self, config):
        Mapper.__init__(self, config)

    def guess_type(self, data):
        topic = data.get("primaryTopic", {})  # will return {}
        # topic = data['primaryTopic'] # will raise exception
        typ = topic.get("type", {})
        typ_uri = typ.get("uri", "")
        if typ_uri == "http://xmlns.com/foaf/0.1/Person":
            topcls = model.Person
        elif typ_uri == "http://xmlns.com/foaf/0.1/Organization":
            topcls = model.Group
        elif typ_uri == "http://RDVocab.info/uri/schema/FRBRentitiesRDA/Family":
            topcls = vocab.Family
        elif data.get("inScheme", {}).get("uri", "") == "http://id.ndl.go.jp/auth#uniformTitles":
            topcls = None
        elif data.get("inScheme", {}).get("uri", "") == "http://id.ndl.go.jp/auth#geographicNames":
            topcls = model.Place
        elif data.get("inScheme", {}).get("uri", "") == "http://id.ndl.go.jp/auth#topicalTerms":
            topcls = model.Type
        else:
            topcls = None
        return topcls

    def do_setup(self, record, rectype=""):
        rec = record["data"]
        guess = self.guess_type(rec)
        if not guess:
            return None
        if not rectype:
            topcls = guess
        elif rectype == "Group" and guess == vocab.Family:
            # this is okay as Family is a Group
            topcls = guess
        else:
            topcls = getattr(model, rectype)
            if topcls != guess:
                print(f"Data in {rec['uri']} is guessed as a {guess} but calling function wants a {rectype}")

        top = topcls(ident=rec["uri"])
        return (top, topcls)

    def handle_common(self, rec, top):
        # Common Features
        pref = rec.get("prefLabel", {})
        preflbl = pref.get("literalForm", "")
        if not preflbl:
            preflbl = rec.get("label", "")
        dupes = {preflbl: 1}

        transcriptions = pref.get("transcription-l", [])
        for txn in transcriptions:
            val = txn.get("@value", "")
            lang = txn.get("@language", "")
            if val and not val in dupes:
                nm = vocab.PrimaryName(content=val)
                lang = lang.split("-", 1)[0]
                lango = self.process_langs.get(lang, None)
                if lango:
                    nm.language = lango
                top.identified_by = nm

        if preflbl:
            top.identified_by = vocab.PrimaryName(content=preflbl)
        else:
            print(f"No primary name in {rec['uri']}?!")

        altLbls = rec.get("altLabel", [])
        if type(altLbls) != list:
            altLbls = [altLbls]
        for alt in altLbls:
            altTxt = alt.get("literalForm", "")
            if not altTxt in dupes:
                dupes[altTxt] = 1
                if altTxt:
                    top.identified_by = vocab.AlternateName(content=altTxt)
            transcriptions = alt.get("transcription-l", [])
            for txn in transcriptions:
                val = txn.get("@value", "")
                lang = txn.get("@language", "")
                if val and not val in dupes:
                    nm = vocab.AlternateName(content=val)
                    lang = lang.split("-", 1)[0]
                    lango = self.process_langs.get(lang, None)
                    if lango:
                        nm.language = lango
                    top.identified_by = nm

        topcls = top.__class__
        equivs = rec.get("exactMatch", [])
        if type(equivs) != list:
            equivs = [equivs]
        for eq in equivs:
            uri = eq.get("uri", "")
            if uri:
                top.equivalent = topcls(ident=uri, label=preflbl)
        return dupes


class JapanMapper(GenericJapanMapper):
    def handle_person(self, rec, top):
        topic = rec.get("primaryTopic", {})
        if not topic:
            return None
        dob = topic.get("dateOfBirth", "")
        if dob:
            ends = make_datetime(dob)
            if ends:
                ts = model.TimeSpan()
                ts.begin_of_the_begin = ends[0]
                ts.end_of_the_end = ends[1]
                ts.identified_by = vocab.DisplayName(content=dob)
                birth = model.Birth()
                birth.timespan = ts
                top.born = birth

        dod = topic.get("dateOfDeath", "")
        if dod:
            ends = make_datetime(dod)
            if ends:
                ts = model.TimeSpan()
                ts.begin_of_the_begin = ends[0]
                ts.end_of_the_end = ends[1]
                ts.identified_by = vocab.DisplayName(content=dod)
                death = model.Death()
                death.timespan = ts
                top.died = death

        fields = topic.get("fieldOfActivityOfThePerson", [])
        if type(fields) != list:
            fields = [fields]
        for field in fields:
            stmt = vocab.BiographyStatement(content=field)
            stmt.language = self.process_langs["ja"]
            top.referred_to_by = stmt

        biogs = topic.get("biographicalInformation", [])
        if type(biogs) != list:
            biogs = [biogs]
        for biog in biogs:
            stmt = vocab.BiographyStatement(content=biog)
            top.referred_to_by = stmt

    def handle_group(self, rec, top):
        #
        # RS, KD (2022-07-11) The right modeling for laterName / previousName
        # is to make a "super" Group that is the continuing action of all
        # variations of the particular company, regardless of its name.
        # BUT ... it's very hard to implement in the pipeline as there isn't
        # a record externally for the super group, nor can we determine an
        # identifier for it.
        #

        topic = rec.get("primaryTopic", {})
        if not topic:
            return None
        dob = topic.get("dateOfEstablishment", "")
        if dob:
            ends = make_datetime(dob)
            if ends:
                ts = model.TimeSpan()
                ts.begin_of_the_begin = ends[0]
                ts.end_of_the_end = ends[1]
                ts.identified_by = vocab.DisplayName(content=dob)
                birth = model.Formation()
                birth.timespan = ts
                top.formed_by = birth

        dod = topic.get("dateOfTermination", "")
        if dod:
            ends = make_datetime(dod)
            if ends:
                ts = model.TimeSpan()
                ts.begin_of_the_begin = ends[0]
                ts.end_of_the_end = ends[1]
                ts.identified_by = vocab.DisplayName(content=dod)
                death = model.Dissolution()
                death.timespan = ts
                top.dissolved_by = death

        hist = topic.get("familyHistory", [])
        if hist:
            if type(hist) != list:
                hist = [hist]
            for h in hist:
                nt = vocab.Note(content=h)
                nt.language = self.process_langs["ja"]
                top.referred_to_by = nt

        hist = topic.get("corporateHistory", [])
        if hist:
            if type(hist) != list:
                hist = [hist]
            for h in hist:
                nt = vocab.Note(content=h)
                nt.language = self.process_langs["ja"]
                top.referred_to_by = nt

        otherRecs = rec.get("previousName", [])
        for orec in otherRecs:
            uri = orec.get("uri", "")
            lbl = orec.get("label", "")
            if uri:
                # We don't have a property for previous name
                # Best we can do is an AttributeAssignment
                aa = model.AttributeAssignment()
                ref = model.Group(ident=uri, label=lbl)
                aa.assigned = ref
                aa.classified_as = model.Type(ident="http://www.wikidata.org/entity/Q79030196", label="Previous Name")
                top.attributed_by = aa

        otherRecs = rec.get("laterName", [])
        for orec in otherRecs:
            uri = orec.get("uri", "")
            lbl = orec.get("label", "")
            if uri:
                # We don't have a property for previous name
                # Best we can do is an AttributeAssignment
                aa = model.AttributeAssignment()
                ref = model.Group(ident=uri, label=lbl)
                aa.assigned = ref
                aa.classified_as = model.Type(ident="http://www.wikidata.org/entity/Q79030284", label="Later Name")
                top.attributed_by = aa

    def handle_place(self, rec, top):
        pass

    def transform(self, record, rectype="", reference=False):
        # Properties to Ignore [RS, KD (2022-07-11)]
        # note - catalogers note, likely to be internal or irrelevant out of context
        # historyNote - history of the record, not the entity described
        # source - not useful
        # inScheme - only useful to the source record, not to the merged entity description
        # created, modified - meta-meta-data about the record, not the entity
        # seeAlso, sameAs, narrower, related, relatedMatch - not relevant or handled by ML
        # dateAssociatedWithCorporateBody - not useful
        # description - not useful

        rec = record["data"]
        (top, topcls) = self.do_setup(record, rectype)
        if not rectype:
            rectype = topcls.__name__
        dupes = self.handle_common(rec, top)

        topic = rec.get("primaryTopic", {})
        topic_name = topic.get("name", "")
        if topic_name and not topic_name in dupes:
            top.identified_by = vocab.AlternateName(content=topic_name)

        otherRecs = rec.get("anotherName", [])
        for orec in otherRecs:
            uri = orec.get("uri", "")
            lbl = orec.get("label", "")
            if uri:
                top.equivalent = topcls(ident=uri, label=lbl)

        realName = rec.get("realName", [])
        for rName in realName:
            uri = rName.get("uri", "")
            lbl = rName.get("label", "")
            if uri:
                top.equivalent = topcls(ident=uri, label=lbl)

        # Class specific features
        fn = getattr(self, f"handle_{rectype.lower()}", None)
        if fn:
            fn(rec, top)

        data = model.factory.toJSON(top)
        return {"identifier": record["identifier"], "data": data, "source": "japan"}


class JapanShMapper(GenericJapanMapper):
    def transform(self, record, rectype="", reference=False):
        rec = record["data"]
        (top, topcls) = self.do_setup(record, rectype)
        self.handle_common(rec, top)

        equivs = rec.get("closeMatch", [])
        if type(equivs) != list:
            equivs = [equivs]
        for eq in equivs:
            uri = eq.get("uri", "")
            if uri:
                top.equivalent = topcls(ident=uri, label="")

        broader = rec.get("broader", [])
        if type(broader) != list:
            broader = [broader]
        for broad in broader:
            uri = broad.get("uri", "")
            label = broad.get("label", "")
            if uri:
                top.broader = topcls(ident=uri, label=label)

        seeAlsos = rec.get("seeAlso", [])
        if seeAlsos:
            if type(seeAlsos) != list:
                seeAlsos = [seeAlsos]
        fetcher = self.configs.external["japansh"]["fetcher"]
        cre = model.Creation()
        top.created_by = cre

        for see in seeAlsos:
            uri = see.get("uri", "")
            # this URI uses the name form, and not the number/identifier
            # so fetch the record, cache it and then use the number
            rest, identifier = uri.rsplit("/", 1)

            # XXX FIXME: We should check that we haven't already fetched the record
            ref_rec = fetcher.fetch(identifier)
            # XXX FIXME: This should cache the record
            ref_uri = ref_rec["data"]["uri"]

            # This is unlikely to matter, as it seems to always only refer to
            # concepts. BUT in case there is a reference to a person, then
            # this code should catch it.
            # FIXME: If it /does/ catch it, then it would already have needed
            #  to use the ndlna fetcher to retrieve it above
            guess = self.guess_type(ref_rec["data"])
            if guess is not None:
                ref = guess(ident=ref_uri, label=identifier)
                cre.influenced_by = ref

        # Class specific features
        fn = getattr(self, f"handle_{rectype.lower()}", None)
        if fn:
            fn(rec, top)

        data = model.factory.toJSON(top)
        return {"identifier": record["identifier"], "data": data, "source": "japan"}
