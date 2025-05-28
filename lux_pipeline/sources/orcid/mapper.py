from lux_pipeline.process.base.mapper import Mapper
from cromulent import model, vocab
from lxml import etree


class OrcidMapper(Mapper):
    def __init__(self, config):
        Mapper.__init__(self, config)
        self.nss = {
            "address": "http://www.orcid.org/ns/address",
            "email": "http://www.orcid.org/ns/email",
            "history": "http://www.orcid.org/ns/history",
            "employment": "http://www.orcid.org/ns/employment",
            "education": "http://www.orcid.org/ns/education",
            "other-name": "http://www.orcid.org/ns/other-name",
            "deprecated": "http://www.orcid.org/ns/deprecated",
            "funding": "http://www.orcid.org/ns/funding",
            "research-resource": "http://www.orcid.org/ns/research-resource",
            "service": "http://www.orcid.org/ns/service",
            "researcher-url": "http://www.orcid.org/ns/researcher-url",
            "distinction": "http://www.orcid.org/ns/distinction",
            "internal": "http://www.orcid.org/ns/internal",
            "membership": "http://www.orcid.org/ns/membership",
            "person": "http://www.orcid.org/ns/person",
            "personal-details": "http://www.orcid.org/ns/personal-details",
            "bulk": "http://www.orcid.org/ns/bulk",
            "common": "http://www.orcid.org/ns/common",
            "record": "http://www.orcid.org/ns/record",
            "keyword": "http://www.orcid.org/ns/keyword",
            "activities": "http://www.orcid.org/ns/activities",
            "qualification": "http://www.orcid.org/ns/qualification",
            "external-identifier": "http://www.orcid.org/ns/external-identifier",
            "error": "http://www.orcid.org/ns/error",
            "preferences": "http://www.orcid.org/ns/preferences",
            "invited-position": "http://www.orcid.org/ns/invited-position",
            "work": "http://www.orcid.org/ns/work",
            "peer-review": "http://www.orcid.org/ns/peer-review",
        }

    def guess_type(self, data):
        return model.Person

    def get_dom(self, record):
        try:
            dom = etree.XML(record["data"]["xml"].encode("utf-8"))
        except Exception as e:
            print(e)
            try:
                dom = etree.XML(record["data"]["xml"])
            except:
                # Some records are garbage HTML?
                # Need to refresh from dump?
                return None
        return dom

    def get_text(self, node, path):
        texts = node.xpath(f"{path}/text()", namespaces=self.nss)
        if texts:
            return self.to_plain_string(texts[0].strip())
        else:
            return None

    def make_webpage(self, url, name="Homepage"):
        lo = model.LinguisticObject()
        do = vocab.WebPage()
        lo.digitally_carried_by = do
        do.format = "text/html"
        do.identified_by = vocab.PrimaryName(content=name)
        do.access_point = model.DigitalObject(ident=url)
        return lo

    def make_date(self, elm):
        y = self.get_text(elm, "./common:year")
        m = self.get_text(elm, "./common:month")
        if m:
            if len(m) == 1:
                m = f"0{m}"
        d = self.get_text(elm, "./common:day")
        if d:
            if len(d) == 1:
                d = f"0{d}"

        if y and m and d:
            dt = f"{y}-{m}-{d}"
        elif y and m:
            dt = f"{y}-{m}-01"
        elif y:
            dt = y
        else:
            # No year??
            return None
        return dt

    def transform(self, record, rectype, reference=False):
        # Data in is XML record from the annual dump file
        # Data loader will strip anything that is just a name and orcid
        # without any additional data, as we don't need records for those.

        # Schema documentation: https://info.orcid.org/documentation/integration-guide/orcid-record/

        if rectype is None:
            rectype = "Person"
        if rectype != "Person":
            return None

        dom = self.get_dom(record)
        if dom is None:
            return None
        rec = dom.xpath("/record:record", namespaces=self.nss)[0]

        orcid = self.get_text(rec, "./common:orcid-identifier/common:uri")
        firstname = self.get_text(rec, "./person:person/person:name/personal-details:given-names")
        famname = self.get_text(rec, "./person:person/person:name/personal-details:family-name")
        name = self.get_text(rec, "./person:person/person:name/personal-details:credit-name")

        if name is None:
            name = f"{firstname} {famname}".strip()
        if not name:
            # I don't think this is possible... but maybe you can set all names private?
            return None

        top = model.Person(ident=orcid, label=name)

        if not reference:
            # Add more features
            nm = vocab.PrimaryName(content=name)
            if firstname:
                nm.part = vocab.GivenName(content=firstname)
            if famname:
                nm.part = vocab.FamilyName(content=famname)
            top.identified_by = nm

            # Other Names as alternate names
            onames = rec.xpath("./person:person/other-name:other-names/other-name:other-name", namespaces=self.nss)
            for o in onames:
                onm = self.get_text(o, "./other-name:content")
                if onm:
                    top.identified_by = vocab.AlternateName(content=onm)

            # Biography
            biog = self.get_text(rec, "./person:person/person:biography/personal-details:content")
            if biog:
                top.referred_to_by = vocab.BiographyStatement(content=biog)

            # URLs
            for rurl in rec.xpath(
                "./person:person/researcher-url:researcher-urls/researcher-url:researcher-url", namespaces=self.nss
            ):
                nm = self.get_text(rurl, "./researcher-url:url-name")
                url = self.get_text(rurl, "./researcher-url:url")
                lo = self.make_webpage(url, name=nm)
                top.subject_of = lo

            # Emails
            for email in rec.xpath("./person:person/email:emails/email:email", namespaces=self.nss):
                em = self.get_text(email, "./email:email")
                if em:
                    top.identified_by = vocab.EmailAddress(content=em)

            # Addresses are only countries, not actual address. Ignore

            # Keywords -- construct a statement
            kws = []
            for kw in rec.xpath("./person:person/keyword:keywords/keyword:keyword", namespaces=self.nss):
                kwtxt = self.get_text(kw, "./keyword:content")
                if kwtxt and not kwtxt in kws:
                    kws.append(kwtxt)
            if kws:
                content = "; ".join(kws)
                top.referred_to_by = vocab.Note(content=f"Interests: {content}")

            # External Identifiers
            for ext in rec.xpath(
                "./person:person/external-identifier:external-identifiers/external-identifier:external-identifier",
                namespaces=self.nss,
            ):
                typ = self.get_text(ext, "./common:external-id-type")
                val = self.get_text(ext, "./common:external-id-value")
                url = self.get_text(ext, "./common:external-id-url")
                # rel will always be self
                if typ == "Scopus Author ID":
                    # Create a web page
                    url = f"https://www.scopus.com/authid/detail.uri?authorId={val}"
                    nm = "Scopus Homepage"
                elif typ == "Loop profile":
                    url = f"http://loop.frontiersin.org/people/{val}/overview"
                    nm = "Loop Homepage"
                elif typ == "SciProfiles":
                    pass  # url is okay as is
                    nm = "SciProfiles Homepage"
                elif typ == "GND":
                    # ahha we can be equivalent!
                    top.equivalent = model.Person(ident=f"https://d-nb.info/gnd/{val}")
                    url = None
                elif typ == "ISNI":
                    # another equivalent
                    top.equivalent = model.Person(ident=f"http://isni.org/isni/{val}")
                    url = None
                else:
                    # kill the rest, including researcherID which is weird UI
                    url = None
                if url is not None:
                    lo = self.make_webpage(url, name=nm)
                    top.subject_of = lo

            ### Activities
            acts = rec.xpath("./activities:activities-summary", namespaces=self.nss)[0]

            # We don't care about works, peer-reviews, invited position, education, qualification
            # service, research-resources

            # Distinctions: To a statement
            # role-title (organization ; start-date - end-date)
            for dist in acts.xpath(
                "./activities:distinctions/activities:affiliation-group/distinction:distinction-summary",
                namespaces=self.nss,
            ):
                role = self.get_text(dist, "./common:role-title")
                if not role:
                    continue
                org = self.get_text(dist, "./common:organization/common:name")
                selm = dist.xpath("./common:start-date", namespaces=self.nss)
                if selm:
                    start = self.make_date(selm[0])
                    eelm = dist.xpath("./common:end-date", namespaces=self.nss)
                    if eelm:
                        end = self.make_date(eelm[0])
                    else:
                        end = None
                else:
                    start = None
                if start:
                    if end:
                        dt = f"{start} - {end}"
                    else:
                        dt = f"{start} - "
                else:
                    dt = ""
                if org and dt:
                    parend = f" ({org} ; {dt})"
                elif org:
                    parend = f" ({org})"
                elif dt:
                    parend = f" ({dt})"
                else:
                    parend = ""
                top.referred_to_by = vocab.Note(content=f"{role}{parend}")

            # Employment: member_of for latest
            for empl in acts.xpath(
                "./activities:employments/activities:affiliation-group/employment:employment-summary",
                namespaces=self.nss,
            ):
                # selm = empl.xpath('./common:start-date', namespaces=self.nss)
                # eelm = empl.xpath('./common:end-date', namespaces=self.nss)
                # Also role-title is the role within the org -- but don't have anywhere to manage these

                orgName = self.get_text(empl, "./common:organization/common:name")
                orgId = self.get_text(
                    empl,
                    "./common:organization/common:disambiguated-organization/common:disambiguated-organization-identifier",
                )
                if orgId and orgName and orgId.startswith("http"):
                    top.member_of = model.Group(ident=orgId, label=orgName)

            # Membership: member_of
            for memb in acts.xpath(
                "./activities:memberships/activities:affiliation-group/membership:membership-summary",
                namespaces=self.nss,
            ):
                orgName = self.get_text(memb, "./common:organization/common:name")
                orgId = self.get_text(
                    memb,
                    "./common:organization/common:disambiguated-organization/common:disambiguated-organization-identifier",
                )
                if orgId and orgName and orgId.startswith("http"):
                    top.member_of = model.Group(ident=orgId, label=orgName)

            # Funding: ??? Can we link this to other data
            # for fund in acts.xpath('./activities:fundings/activities:group/funding:funding-summary', namespaces=self.nss):
            # fname = self.get_text(fund, './funding:title')
            # type = grant_number
            # fid = self.get_text(fund, './common:external-ids/common:external-id/common:external-id-value')
            # start, end
            # org = funding org
            # But then this would need to be a separate record ... ignore for now

        data = model.factory.toJSON(top)
        recid = record["identifier"]
        return {"identifier": record["identifier"], "data": data, "source": "orcid"}
