from pipeline.process.base.loader import Loader
import zipfile
from lxml import etree

class FastLoader(Loader):
    def __init__(self, config):
        self.in_url = config.get("remoteDumpFile", "")
        self.in_path = config["dumpFilePath"]
        self.out_cache = config["datacache"]
        self.config = config
        self.configs = config["all_configs"]

    def load(self):
        nss = {"mx": "http://www.loc.gov/MARC21/slim"}
        records_processed = 0

        with zipfile.ZipFile(self.in_path, "r") as fh:
            for fn in fh.namelist():
                if not fn.endswith(".marcxml"):
                    continue
                print(f"Processing file: {fn}")
                
                with fh.open(fn) as f:
                    try:
                        tree = etree.parse(f)
                        root = tree.getroot()
                        
                        for record in root.findall(".//mx:record", namespaces=nss):
                            control_001 = record.find('.//mx:controlfield[@tag="001"]', namespaces=nss)
                            df682 = record.find(".//mx:datafield[@tag='682']",namespaces=nss)
                            if df682:
                                #do not load deleted records
                                for df in df682:
                                    subfield_i = df.find("mx:subfield[@code='i']", namespaces=self.nss)
                                    delete_text = str(subfield_i.text) if subfield_i.text is not None else ""
                                    if delete_text and "deleted" in delete_text:
                                        continue

                            elif control_001 is not None and control_001.text:
                                ident = control_001.text.split("fst")[-1]
                                ident = ident.lstrip("0") if ident.startswith("0") else ident

                                self.out_cache[ident] = {"xml": etree.tostring(record, encoding="unicode")}
                                records_processed += 1
                            else:
                                print(f"FAST record in {fn} is missing an identifier.")
                                continue

                    except etree.XMLSyntaxError as e:
                        print(f"Error parsing XML in {fn}: {e}")

        if records_processed > 0:
            self.out_cache.commit()
            print(f"Successfully loaded {records_processed} records into cache.")
        else:
            print("No valid records were processed.")