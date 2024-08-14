from pipeline.process.reidentifier import Reidentifier
from pipeline.process.base.mapper import Mapper
from pipeline.process.utils.mapper_utils import test_birth_death
from urllib.parse import quote
import ujson as json
import os


class Cleaner(Mapper):
    def __init__(self, config):
        Mapper.__init__(self, config)

        self.configs = config["all_configs"]
        self.globals = self.configs.globals
        self.wikimedia = self.configs.external["wikimedia"]

        idmap = self.configs.get_idmap()

        self.reidentifier = Reidentifier(self.configs, idmap)
        self.idmap = idmap
        self.metatypes = {}
        self.metatype_yuid_cache = {}

        fn = os.path.join(self.configs.data_dir, "metatypes.json")
        if os.path.exists(fn):
            fh = open(fn)
            data = fh.read()
            fh.close()
            self.metatypes = json.loads(data)

    def get_commons_license(self, img):
        # Can't store reidentified version as it would need a YUID
        # And YUIDs must be UUIDs - no way to look up fn->yuid
        # without stuffing them in the idmap, and that would be a waste
        if not img in self.wikimedia["recordcache"]:
            if not img in self.wikimedia["datacache"]:
                data = self.wikimedia["fetcher"].fetch(img)
                if data:
                    self.wikimedia["datacache"][img] = data
                else:
                    return None
            else:
                data = self.wikimedia["datacache"][img]
            la = self.wikimedia["mapper"].transform(data, "")
            if not la:
                self.wikimedia["recordcache"][img] = {"data": {}, "identifier": img}
                return None
            else:
                self.wikimedia["recordcache"][img] = la
        else:
            la = self.wikimedia["recordcache"][img]
            if not la["data"]:
                return None
        # thankfully reidentify is fast, so just do it every time
        la2 = self.reidentifier._reidentify(la, "DigitalObject", True)
        return la2

    def process_images(self, data):
        if "representation" in data:
            # Try and manage wikimedia images all at once

            all_reps = data["representation"][:]
            wd_aps = []
            ap_reps = {}
            for rep in all_reps:
                try:
                    apid = rep["digitally_shown_by"][0]["access_point"][0]["id"]
                except:
                    # trash it
                    continue

                # Munge stupid wikimedia image URLs
                if "commons.wikimedia.org/wiki/special:filepath" in apid.lower():
                    if apid.startswith("http:"):
                        apid = apid.replace("http://", "https://")
                    url, fn = apid.rsplit("/", 1)
                    # Special:FilePath isn't case sensitive so normalize to all lower
                    url = url.lower()

                    if "?" in fn:
                        # strip ?width=n (etc)
                        fn = fn.rsplit("?", 1)[0]
                    elif "%3fwidth=" in fn:
                        fn = fn.rsplit("%3fwidth=", 1)[0]
                    fn = fn.replace("%20", "_")
                    if not "%" in fn:
                        fn = quote(fn.strip())

                    if fn:
                        # Now look up license etc
                        do = self.get_commons_license(fn)
                        del rep["digitally_shown_by"]
                        if do:
                            rep["digitally_shown_by"] = [do["data"]]
                        else:
                            continue
                elif not "yale.edu" in apid.lower():
                    # Trash them as we can't validate licenses
                    continue

                if not apid in ap_reps:
                    ap_reps[apid] = rep

            # Now try to filter out the same image with minor variations
            # prefer cropped over not
            for w in wd_aps:
                cidx = w.find("cropped")
                if cidx > -1:
                    # Try to find base
                    w = w.replace("cropped", "")
                    w = w.replace("%28%29", "")  # ()
                    w = w.replace("%5B%5D", "")  # []
                    w = w.replace("_.", ".")
                    if w in ap_reps:
                        del ap_reps[w]

            data["representation"] = []
            for v in ap_reps.values():
                data["representation"].append(v)

    def process_names(self, data):
        primary = self.globals["primaryName"]  # 300404670
        sortName = self.globals["sortName"]  # 300404672
        primaryType = {
            "id": primary,
            "type": "Type",
            "_label": "Primary Name",
            "equivalent": [
                {"id": "http://vocab.getty.edu/aat/300404670", "type": "Type", "_label": "Final: Primary Name"}
            ],
        }
        sortType = {
            "id": sortName,
            "type": "Type",
            "_label": "Sort Name",
            "equivalent": [{"id": "http://vocab.getty.edu/aat/300451544", "type": "Type", "_label": "Final: Sort Title"}],
        }

        alternateName = self.globals["alternateName"]  # 300264273
        alternateTitle = self.globals["alternateTitle"]  # 300417227
        translatedTitle = self.globals["translatedTitle"]  # 300417194
        officialName = self.globals["officialName"]  # 300404686

        english = self.globals["lang_en"]
        spanish = self.globals["lang_es"]
        french = self.globals["lang_fr"]
        german = self.globals["lang_de"]
        dutch = self.globals["lang_nl"]
        chinese = self.globals["lang_zh"]

        # displayName = self.globals['displayName'] # 300404669
        # original title # 300417204
        # HTML # 300266021
        # published title # 300417206
        # common name # 300404687

        # Trash temporary names, ensure primary name
        # ensure one sortName, prefer unit given sort name, then english primary name,
        #     then primary name with no language, otherwise first primary name, whatever that is
        # ensure no primary+alternate name

        if "identified_by" in data:
            lang_names = {}
            remove = []
            # invert the names into languages then primary / not primary
            for nm in data["identified_by"]:
                if nm["type"] == "Name":
                    # FIXME: Test for 'language': [{'type': 'Language', '_label': 'und'}]
                    langs = [x.get("id", None) for x in nm.get("language", [{"id": None}])]
                    val = nm.get("content", "")
                    val = val.strip()
                    if not val:
                        remove.append(nm)
                    else:
                        for l in langs:
                            try:
                                lang_names[l].append(nm)
                            except:
                                lang_names[l] = [nm]
            if remove:
                for r in remove:
                    data["identified_by"].remove(r)

            # Now set a primary name for each language (including no language)
            # Ensure no alternate and primary

            sort_name_langs = {}
            primary_name_langs = {}

            # lang_names = {None: [{pn}, {sn}]}

            for lang, nms in lang_names.items():
                has_sort = False
                primaryNameVals = []
                for nm in nms:
                    cxns = [x.get("id", None) for x in nm.get("classified_as", [])]
                    if None in cxns:
                        print(f" ---> {data['id']} has {nm['classified_as']} name cxns")
                    if primary in cxns and alternateName in cxns:
                        if primaryNameVals:
                            # make it alternate
                            rem = None
                            for c in nm["classified_as"]:
                                if c["id"] == primary:
                                    rem = c
                                    break
                            nm["classified_as"].remove(rem)
                        else:
                            # make it primary
                            rem = None
                            for c in nm["classified_as"]:
                                if c["id"] == alternateName:
                                    rem = c
                                    break
                            nm["classified_as"].remove(rem)
                            primaryNameVals.append(nm)
                    elif primary in cxns:
                        primaryNameVals.append(nm)
                    if sortName in cxns:
                        try:
                            sort_name_langs[lang].append(nm)
                        except:
                            sort_name_langs[lang] = [nm]

                # primaryNameVals = [{pn}]
                # sort_name_langs[None] = [{sn}]

                if not primaryNameVals:
                    if len(nms) == 1:
                        candidates = nms
                    else:
                        # FIXME: Any other heuristics here to make a better guess?
                        candidates = []
                        for nm in nms:
                            cxns = [x.get("id", None) for x in nm.get("classified_as", [])]
                            if not cxns:
                                candidates.insert(0, nm)
                            else:
                                if officialName in cxns:
                                    candidates = [nm]
                                    break
                                alt = False
                                for a in [alternateName, alternateTitle, translatedTitle]:
                                    if a in cxns:
                                        # Don't add explicit alternates
                                        alt = True
                                        break
                                if not alt:
                                    candidates.append(nm)
                        # Shorter makes for a better web page title?

                        candidates.sort(key=lambda x: len(x["content"]))
                        # Places have "names" of the two letter code that are worse than longer ones
                        if len(candidates) > 1 and data["type"] == "Place":
                            if len(candidates[0]["content"]) < 3:
                                candidates = candidates[1:] + [candidates[0]]

                    if not candidates:
                        # Everything was bad :(
                        target = nms[0]
                        done = False
                        cxns = [x.get("id", None) for x in target.get("classified_as", [])]
                        for a in [alternateName, alternateTitle, translatedTitle]:
                            if a in cxns:
                                # Gah. Overwrite. We need a primary name
                                target["classified_as"] = [primaryType]
                                done = True
                        if not done:
                            if not "classified_as" in target:
                                target["classified_as"] = []
                        target["classified_as"].append(primaryType)
                    else:
                        target = candidates[0]
                        if not "classified_as" in target:
                            target["classified_as"] = []
                        else:
                            # remove alternate if present
                            remove = []
                            for cx in target["classified_as"]:
                                if "id" in cx and cx["id"] in [alternateName, alternateTitle]:
                                    remove.append(cx)
                            for r in remove:
                                target["classified_as"].remove(r)

                        target["classified_as"].append(primaryType)
                    primaryNameVals = [target]
                    primary_name_langs[lang] = target
                elif len(primaryNameVals) > 1:
                    # pick shortest, and de-primary the others
                    primaryNameVals.sort(key=lambda x: len(x["content"]))
                    if len(primaryNameVals) > 1 and data["type"] == "Place":
                        if len(primaryNameVals[0]["content"]) < 3:
                            primaryNameVals = primaryNameVals[1:] + [primaryNameVals[0]]

                    # test for acronyms... prefer Great Britain to GB,
                    #   International Businesss Machines to IBM
                    if primaryNameVals[0]["content"].isupper():
                        acrs = []
                        other = []
                        for p in primaryNameVals:
                            if p["content"].isupper():
                                acrs.append(p)
                            else:
                                other.append(p)
                        primaryNameVals = other
                        primaryNameVals.extend(acrs)

                    for nm in primaryNameVals[1:]:
                        if len(nm["classified_as"]) == 1:
                            del nm["classified_as"]
                        else:
                            remove = []
                            for cx in nm["classified_as"]:
                                if "id" in cx and cx["id"] == primary:
                                    remove.append(cx)
                            for r in remove:
                                nm["classified_as"].remove(r)
                    primary_name_langs[lang] = primaryNameVals[0]
                else:
                    primary_name_langs[lang] = primaryNameVals[0]

            if sort_name_langs:
                if len(sort_name_langs) == 1:
                    sort_name = list(sort_name_langs.values())[0][0]
                elif english in sort_name_langs:
                    sort_name = sort_name_langs[english][0]
                elif None in sort_name_langs:
                    sort_name = sort_name_langs[None][0]
                else:
                    sort_name = list(sort_name_langs.values())[0][0]
                # remove sort name from any extra sort names
                for v in sort_name_langs.values():
                    for n in v:
                        if n is not sort_name:
                            remove = []
                            for cx in n["classified_as"]:
                                if "id" in cx and cx["id"] == sortName:
                                    remove.append(cx)
                            for r in remove:
                                n["classified_as"].remove(r)
            else:
                # get english primary name
                if english in primary_name_langs:
                    target = primary_name_langs[english]
                elif None in primary_name_langs:
                    target = primary_name_langs[None]
                elif primary_name_langs:
                    target = list(primary_name_langs.values())[0]
                else:
                    # no primary names, just skip
                    target = {}
                if "classified_as" in target:
                    target["classified_as"].append(sortType)

        if (not "identified_by" in data or not data["identified_by"]) and "_label" in data and data["_label"]:
            # copy label to name
            data["identified_by"] = [{"type": "Name", "content": data["_label"], "classified_as": [primaryType]}]
        elif not "identified_by" in data and data["type"] == "DigitalObject" and len(data.keys()) == 4:
            # bad record ... just a pointer to some other URI (id, type, _label, equivalent)
            return None
        elif (
            not "identified_by" in data
            or not data["identified_by"]
            or (len(data["identified_by"]) == 1 and not data["identified_by"][0].get("content", ""))
        ):
            # Uh oh :(
            print(f"record with no names: {data}")
            data["identified_by"] = [
                {"type": "Name", "classified_as": [primaryType], "content": f"Unnamed {data['type']}"}
            ]

        # Now sort names

        def score_name(nm):
            # english, spanish, french, others
            # primary first
            if nm["type"] == "Identifier":
                return 0
            cxns = [x["id"] for x in nm.get("classified_as", []) if "id" in x]
            langs = [x["id"] for x in nm.get("language", []) if "id" in x]
            if english in langs:
                t = 100
            elif spanish in langs:
                t = 90
            elif french in langs:
                t = 80
            elif german in langs:
                t = 70
            elif dutch in langs:
                t = 60
            elif chinese in langs:
                t = 50
            elif langs:
                t = 10
            else:
                t = 0
            if primary in cxns:
                t += 5
            elif alternateName in cxns:
                t += 1
            return t

        data["identified_by"].sort(key=score_name, reverse=True)
        return True

    def dedupe_properties(self, data, prop):
        counter = {}
        replacement = []
        for c in data.get(prop, []):
            c_id = c.get("id", "")
            if c_id and c_id not in counter:
                counter[c_id] = 1
                replacement.append(c)
        if replacement:
            data[prop] = replacement

    def ensure_timespans(self, et):
        # ensure if begin_of_begin then end_of_end and vice versa
        if "timespan" in et:
            if "begin_of_the_begin" in et["timespan"] and "end_of_the_end" not in et["timespan"]:
                et["timespan"]["end_of_the_end"] = "9999-12-31T23:59:59"
            elif "end_of_the_end" in et["timespan"] and not "begin_of_the_begin" in et["timespan"]:
                et["timespan"]["begin_of_the_begin"] = "-9999-01-01T00:00:00"

        elif "part" in et:
            for part in et["part"]:
                if "timespan" in part:
                    if "begin_of_the_begin" in part["timespan"] and "end_of_the_end" not in part["timespan"]:
                        part["timespan"]["end_of_the_end"] = "9999-12-31T23:59:59"
                    elif "end_of_the_end" in part["timespan"] and not "begin_of_the_begin" in part["timespan"]:
                        part["timespan"]["begin_of_the_begin"] = "-9999-01-01T00:00:00"

    def check_for_metatypes(self, data):
        if "equivalent" in data:
            for eq in data["equivalent"]:
                if eq["id"] in self.metatypes:
                    # add the metatypes to classified_as
                    if not "classified_as" in data:
                        data["classified_as"] = []
                    curr = [x["id"] for x in data["classified_as"] if "id" in x]
                    for md in self.metatypes[eq["id"]]:
                        # this is after reidentifier, so need to add the mapped YUID
                        if md in self.metatype_yuid_cache:
                            mdy = self.metatype_yuid_cache[md]
                        else:
                            if not self.configs.is_qua(md):
                                md = self.configs.make_qua(md, "Type")
                            mdy = self.idmap[md]
                            self.metatype_yuid_cache[md] = mdy
                        if not mdy in curr:
                            # Find the AAT equivalent
                            aat = self.configs.split_qua(md)[0]
                            data["classified_as"].append(
                                {
                                    "id": mdy,
                                    "type": "Type",
                                    "_label": "Metatype",
                                    "equivalent": [{"id": aat, "type": "Type", "_label": "Metatype"}],
                                }
                            )

    def dedupe_webpages(self, data):
        webs = data["subject_of"]
        if len(webs) < 2:
            # Nothing to do
            return
        aps = []
        ws = {}
        okay = []
        # get all the aps and the web blocks, stuff in appropriate arrays
        for web in webs:
            if "digitally_carried_by" in web:
                for points in web["digitally_carried_by"]:
                    if "access_point" in points and "id" in points["access_point"][0]:
                        ap = points["access_point"][0]["id"]
                        aps.append(ap)
                        ws[ap] = web

        del data["subject_of"]

        ### FIXME: this still doesn't work correctly
        # Lots of triggers to the except block at line 504
        # where it can't find the URL selected

        for a in aps:
            http = a.replace("http://", "https://", 1)
            https = a.replace("https://", "http://", 1)
            opts = [a, http, https]
            for o in opts[:]:
                opts.append(o.replace("//www.", "//", 1))
            for o in opts[:]:
                if o[-1] == "/":
                    opts.append(o[:-1])
                else:
                    opts.append(f"{o}/")

            found = False
            for o in opts:
                if o and o in okay:
                    found = True
                    break
            if not found:
                https_found = False
                for o in opts:
                    if o and o.startswith("https://"):
                        okay.append(o)
                        https_found = True
                        break
                if not https_found:
                    okay.append(a)

        subj = []
        # okay should never be empty
        for k in okay:
            try:
                block = ws[k]
                subj.append(block)
            except:
                print(f"\n ------- Could not find {k} in {ws} for {data['id']}")
                # pass
        if subj:
            data["subject_of"] = subj

    def transform(self, rec, rectype=None, reference=False):
        data = rec["data"]

        ### Deduplicate properties
        propList = ["classified_as, represents, part_of, made_of, member_of"]
        for p in propList:
            self.dedupe_properties(data, p)

        if data["type"] in ["Person", "Group", "Place"]:
            if "subject_of" in data:
                self.dedupe_webpages(data)

        eventTypes = ["produced_by", "used_for", "created_by", "born", "died", "formed_by", "dissolved_by"]
        for et in eventTypes:
            if et in data:
                self.ensure_timespans(data[et])

        ### Check names are sane
        okay = self.process_names(data)
        if not okay:
            print(f"Final mapper failed to find sane names in {data['id']}, dropping")
            return None
        self.process_images(data)
        self.check_for_metatypes(data)

        if data["type"] == "Person":
            okay = test_birth_death(data)
            if not okay:
                try:
                    del data["born"]
                    del data["died"]
                except:
                    # This shouldn't ever happen, but not going to die on the hill
                    pass

        # prevent self-referential partitioning
        for p in ["broader", "part_of", "member_of"]:
            if p in data:
                kill = []
                for what in data[p]:
                    if "id" in what and what["id"] == data["id"]:
                        kill.append(what)
                for k in kill:
                    data[p].remove(k)

        # Trash (mostly) YUL Place parents if we have merged in a better one
        if data["type"] == "Place" and "part_of" in data and len(data["part_of"]) > 1:
            # Look up in idmap to see if YUL only
            for parent in data["part_of"].copy():
                try:
                    equivs = self.idmap[parent["id"]]
                except:
                    continue
                okay = False
                for eq in equivs:
                    if (
                        "whosonfirst" in eq
                        or "geonames" in eq
                        or "wikidata" in eq
                        or "getty" in eq
                        or "loc.gov" in eq
                        or "viaf" in eq
                    ):
                        okay = True
                        break
                if not okay:
                    data["part_of"].remove(parent)

        data["@context"] = "https://linked.art/ns/v1/linked-art.json"
        return rec
