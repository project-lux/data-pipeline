import os
import ujson as json
import re
from cromulent import model, vocab
from lxml import etree

from pipeline.process.utils.mapper_utils import make_datetime
from pipeline.process.utils.xpath_ops import process_operation

model.ExternalResource._write_override = None
# monkey patch in members_exemplified_by for Set and Group
mebInfo = model.PropInfo("members_exemplified_by", "la:members_exemplified_by", model.CRMEntity, "", None, 1, 1)
model.Set._all_properties["members_exemplified_by"] = mebInfo
model.Group._all_properties["members_exemplified_by"] = mebInfo


class Mapper(object):
    def __init__(self, config):
        # Not sure if this is useful, but worth configuration once
        self.factory = model.factory
        self.factory.auto_assign_id = False
        self.factory.validate_properties = False
        self.factory.validate_profile = False
        self.factory.validate_range = False
        self.factory.validate_multiplicity = False
        self.factory.json_serializer = "fast"
        self.factory.order_json = False
        self.factory.cache_hierarchy()

        self.process_langs = {}
        self.aat_material_ids = []
        self.aat_unit_ids = []
        for l, i in vocab.identity_instances.items():
            if i["parent"] == model.Language:
                self.process_langs[i["code"]] = vocab.instances[l]
            elif i["parent"] == model.Material:
                self.aat_material_ids.append(vocab.instances[l].id)
            elif i["parent"] == model.MeasurementUnit:
                self.aat_unit_ids.append(vocab.instances[l].id)

        self.lang_three_to_two = {
            "por": "pt",
            "deu": "de",
            "ger": "de",
            "eng": "en",
            "fra": "fr",
            "fre": "fr",
            "spa": "es",
            "zho": "zh",
            "chi": "zh",
            "hin": "hi",
            "afr": "af",
            "alb": "sq",
            "sqi": "sq",
            "ara": "ar",
            "bul": "bg",
            "bos": "bs",
            "cat": "ca",
            "ben": "bn",
            "rus": "ru",
            "nld": "nl",
            "dut": "nl",
            "fin": "fi",
            "ile": "is",
            "gle": "ga",
            "ita": "it",
            "fas": "fa",
            "per": "fa",
            "guj": "gu",
            "kor": "ko",
            "lat": "la",
            "lit": "lt",
            "mac": "mk",
            "mkd": "mk",
            "jpn": "ja",
            "hrv": "hr",
            "ces": "cs",
            "cze": "cs",
            "dan": "da",
            "ell": "el",
            "gre": "el",
            "kat": "ka",
            "geo": "ka",
            "heb": "he",
            "hun": "hu",
            "nor": "no",
            "pol": "pl",
            "ron": "ro",
            "rum": "ro",
            "slk": "sk",
            "slo": "sk",
            "slv": "sl",
            "srp": "sr",
            "swe": "sv",
            "tur": "tr",
            "cym": "cy",
            "wel": "cy",
            "urd": "ur",
            "swa": "sw",
            "ind": "id",
            "tel": "te",
            "tam": "ta",
            "tha": "th",
            "mar": "mr",
            "pan": "pa",
        }

        self.must_have = ["en", "es", "fr", "pt", "de", "nl", "zh", "ja", "ar", "hi"]

        self.cycle_breaks = {}
        # Read from file
        if "cycleBreakPath" in config and os.path.exists(config["cycleBreakPath"]):
            with open(config["cycleBreakPath"]) as fh:
                self.cycle_breaks = json.load(fh)

        self.config = config
        self.configs = config["all_configs"]
        idmap = self.configs.get_idmap()
        fn = os.path.join(self.configs.data_dir, "type_overrides.json")
        self.type_overrides = {}
        if os.path.exists(fn):
            with open(fn) as fh:
                self.type_overrides = json.load(fh)

        # Mapping might need preferred URI for source
        self.namespace = config["namespace"]
        self.name = config["name"]
        self.globals = self.configs.globals
        self.global_reconciler = self.configs.results["merged"].get("reconciler", None)
        self.debug = False
        self.acquirer = None

        self.xpath_fixes = {}
        fn = os.path.join(self.configs.data_dir, f"xpath_fixes.json")
        if os.path.exists(fn):
            eg = etree.XML("<record/>")
            with open(fn) as fh:
                data = json.load(fh)
            for f in data:
                if f["source"] == self.name:
                    which = "identifier" if f["identifier"] else "equivalent"
                    if f["identifier"]:
                        ident = f["identifier"]
                    elif f["equivalent"]:
                        equivs = f["equivalent"].split(" ")
                        yuids = []
                        for eq in equivs:
                            try:
                                qua = self.configs.make_qua(eq, f["class"])
                                yuid = idmap[qua]
                            except:
                                yuid = None
                            if not yuid:
                                print(f"Failed to find record for equivalent: {qua}")
                            else:
                                yuids.append(yuid)
                        if len(set(yuids)) != 1:
                            print(f"Failed to find single YUID for {qua}: {yuids}")
                        else:
                            ident = yuids[0][-36:]

                    if not ident:
                        print(f"{self.name} xpath fix has no identifier or equivalent: {f}")
                        continue
                    if not f["path"]:
                        print(f"{self.name} xpath fix has no xpath: {f}")
                        continue
                    try:
                        eg.xpath(f["path"])
                    except:
                        print(f"{self.name} xpath is not parsable: {f}")
                        continue
                    try:
                        self.xpath_fixes[ident].append(f)
                    except:
                        self.xpath_fixes[ident] = [f]

        self.single_century_regex = re.compile(r"(\d{1,2})(?:st|nd|rd|th) century$")
        self.range_centuries_regex = re.compile(r"(\d{1,2})(?:st|nd|rd|th) century.*?(\d{1,2})(?:st|nd|rd|th) century")

    def process_period_record(self, record):
        # Add AAT classification
        record.setdefault("classified_as", []).append({
            "id": "http://vocab.getty.edu/aat/300081446",
            "type": "Type",
            "_label": "Period",
        })
        
        if "timespan" not in record:
            identified_by = record.get("identified_by", [])
            for identifier in identified_by:
                classified_as = identifier.get("classified_as", [])
                for cxn in classified_as:
                    if cxn.get("id") == "http://vocab.getty.edu/aat/300404670":
                        content = identifier.get("content", "")

                        if "," in cont:
                            #Library periods
                            dates = cont.rsplit(",", 1)[-1].strip()
                        if self.single_century_regex.match(content):
                            #Museum periods
                            century = int(self.single_century_regex.match(content).group(1))
                            start_year, end_year = (century - 1) * 100, (century - 1) * 100 + 99
                            dates = f"{start_year} - {end_year}"
                        elif self.range_century_regex.match(content):
                            start_century, end_century = map(
                                int, self.range_century_regex.match(content).groups()
                            )
                            start_year, end_year = (start_century - 1) * 100, (end_century - 1) * 100 + 99
                            dates = f"{start_year} - {end_year}"
                        else:
                            dates = None

                        if dates:
                            try:
                                begin, end = make_datetime(dates)
                            except:
                                begin = end = None
                            if begin and end:
                                record["timespan"] = {
                                    "type": "TimeSpan",
                                    "begin_of_the_begin": begin,
                                    "end_of_the_end": end,
                                    "identified_by": [{
                                        "type": "Name",
                                        "classified_as": [{
                                            "id": "http://vocab.getty.edu/aat/300404669",
                                            "type": "Type",
                                            "_label": "Display Title",
                                        }],
                                        "content": content,
                                    }]
                                }
                        break
        return record

    def returns_multiple(self, record=None):
        return False

    def should_merge_into(self, base, merge):
        return True

    def should_merge_from(self, base, merge):
        return True

    def fix_identifier(self, identifier):
        return identifier

    def expand_uri(self, identifier):
        return self.namespace + identifier
    
    def to_plain_string(self, value):
        return str(value) if isinstance(value, etree._ElementUnicodeResult) else value

    def get_reference(self, identifier):
        if not self.acquirer:
            self.acquirer = self.config["acquirer"]
        try:
            fetchedrec = self.acquirer.acquire(identifier, reference=True)
        except:
            return None
        if fetchedrec is not None:
            rectype = fetchedrec["data"]["type"]
            crmcls = getattr(model, rectype)
            return crmcls(ident=self.expand_uri(identifier), label=fetchedrec["data"].get("_label", ""))
        else:
            return None

    def _walk_fix_links(self, node, topid):
        if "id" in node and node["id"] != topid:
            uri = self.configs.canonicalize(node["id"])
            if uri != node["id"]:
                if not uri:
                    # print(f"Unsetting bad node id: {node['id']} in {topid}")
                    del node["id"]
                else:
                    node["id"] = uri

        for k, v in node.items():
            if k in ["equivalent", "access_point", "conforms_to"]:
                continue
            if type(v) == list:
                for vi in v:
                    if type(vi) == dict:
                        self._walk_fix_links(vi, topid)
            elif type(v) == dict:
                self._walk_fix_links(v, topid)

    def fix_links(self, record, walk=True):
        if record is not None:
            if "data" in record:
                data = record["data"]
            else:
                data = record
            if "equivalent" in data:
                new_eqs = []
                for eq in data["equivalent"]:
                    if "id" in eq:
                        uri = self.configs.canonicalize(eq["id"])
                        if uri:
                            # ensure type and _label while we're at it
                            neq = {"id": uri}
                            neq["type"] = eq.get("type", data["type"])
                            neq["_label"] = eq.get("_label", data.get("_label", "Equivalent"))
                            new_eqs.append(neq)
                data["equivalent"] = new_eqs
            if walk:
                if "id" in data:
                    self._walk_fix_links(data, data["id"])
                else:
                    print(f"Found record without a URI?? {data}")
        return record

    def break_cycles(self, record, xformtype):
        try:
            recid = record["data"]["id"]
        except:
            return record
        if xformtype is None:
            xformtype = record["data"]["type"]
        qrecid = self.configs.make_qua(recid, xformtype)
        qrecid = qrecid.replace(self.namespace, "")
        # self.cycle_breaks = {uri-of-part: [uri-of-parent-to-remove, uri-of-parent-to-remove,...]}
        if qrecid in self.cycle_breaks:
            typ = record["data"]["type"]
            if typ in ["Place", "Group", "Type", "Language", "Material", "MeasurementUnit", "Currency"]:
                if typ == "Place":
                    prop = "part_of"
                elif typ == "Group":
                    prop = "member_of"
                else:
                    prop = "broader"
            if prop in record["data"]:
                for remove in self.cycle_breaks[qrecid]:
                    for p in record["data"][prop][:]:
                        if "id" in p and p["id"] == remove:
                            print(f"Broke a cycle: {p} from {recid}")
                            record["data"][prop].remove(p)
                            break
                if not record["data"][prop]:
                    del record["data"][prop]
        return record

    def trash_different_from(self, record):
        if self.global_reconciler is None:
            self.global_reconciler = self.configs.results["merged"]["reconciler"]
        try:
            recid = record["data"]["id"]
        except:
            return record
        if "equivalent" in record["data"]:
            hsh = {e["id"]: e for e in record["data"]["equivalent"]}
            diffs = self.global_reconciler.reconcile(recid, "diffs")
            for d in diffs:
                if d in hsh:
                    print(f"Removed a different from: {d} from {recid}")
                    record["data"]["equivalent"].remove(hsh[d])
        return record

    def process_xpath_fixes(self, record):
        if self.xpath_fixes:
            if "identifier" in record:
                ident = record["identifier"]
            else:
                ident = record["yuid"]
            fixes = self.xpath_fixes.get(ident, [])
            for fix in fixes:
                p = fix["path"]  # now a parsed path
                op = fix["operation"]
                arg = fix.get("argument", None)
                if arg is not None:
                    if arg.startswith("{") and arg.endswith("}"):
                        # parse from string to json
                        arg = json.loads(arg)
                    elif arg.startswith('"') and arg.endswith('"'):
                        # strip quotes
                        arg = arg[1:-1]
                    elif arg.replace(".", "", 1).isnumeric() and p.endswith("/value"):
                        arg = float(arg)
                process_operation(record["data"], p, op, arg)
        return record

    def post_mapping(self, record, xformtype=None):
        record = self.fix_links(record)
        if record is None and self.debug:
            print(f"fix_links killed record")
        record = self.break_cycles(record, xformtype)
        if record is None and self.debug:
            print(f"break_cycles killed record")
        record = self.trash_different_from(record)
        if record is None and self.debug:
            print(f"trash_different killed record")
        record = self.process_xpath_fixes(record)
        if record is None:
            print(f"xpath_fixes trashed the record :(")
        return record

    def post_reconcile(self, record):
        if record["data"]["type"] == "Type":
            fequivs = [x["id"] for x in record["data"].get("equivalent", [])]
            for fe in fequivs:
                if fe in self.type_overrides:
                    record["data"]["type"] = self.type_overrides[fe]
                    break

    def transform(self, record, rectype, reference=False):
        # No op
        # This almost certainly needs to be overridden
        return record


class MultiMapper(Mapper):
    # A mapper that will return a list of extracted records via transform_all
    # Or only the "main" record via transform

    def returns_multiple(self, record=None):
        return True

    def transform_all(self, record):
        return [record]
