from pipeline.process.base.loader import Loader
import zipfile
from lxml import etree

class FastLoader(Loader):
    """
    A specialized loader for processing MARCXML files contained within a ZIP archive.

    This loader extracts MARC21 records, filters out deleted records, and stores valid records 
    in the specified cache. It reads XML data from compressed files, ensuring efficient processing.

    Attributes:
        in_url (str): URL of the remote dump file (if applicable).
        in_path (str): Path to the local ZIP file containing MARCXML data.
        out_cache (dict-like): A cache for storing processed records.
        config (dict): The full configuration dictionary.
        configs (dict): Additional configuration settings.
    """

    def __init__(self, config):

        """
        Initializes the FastLoader with the provided configuration.

        Args:
            config (dict): A dictionary containing required paths and cache settings.
                - "dumpFilePath" (str): Local path to the ZIP archive.
                - "datacache" (dict-like): Storage for parsed records.
        """

        self.in_path = config["dumpFilePath"]
        self.out_cache = config["datacache"]

    def load(self):

        """
        Loads MARCXML records from the ZIP archive, processes them, and stores them in the cache.

        The method iterates through XML files in the ZIP, extracts relevant MARC21 records,
        and filters out records marked as "deleted" in field 682. It then stores the processed
        records in `out_cache`, using the control field 001 as the identifier.

        Processing steps:
        - Open and read ZIP file.
        - Extract MARCXML files.
        - Parse XML, locate records.
        - Skip records marked as deleted.
        - Store valid records in the cache.
        - Commit the cache changes.

        Raises:
            FileNotFoundError: If the ZIP file is not found.
            zipfile.BadZipFile: If the ZIP file is corrupted.
        """

        nss = {"mx": "http://www.loc.gov/MARC21/slim"}
        records_processed = 0

        try:
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
                                df682 = record.findall(".//mx:datafield[@tag='682']", namespaces=nss)

                                # Skip deleted records
                                if df682:
                                    for df in df682:
                                        subfield_i = df.find(".//mx:subfield[@code='i']", namespaces=nss)
                                        delete_text = subfield_i.text if subfield_i is not None else ""
                                        if "deleted" in delete_text.lower():
                                            continue

                                if control_001 is not None and control_001.text:
                                    ident = control_001.text
                                    if "fst" in ident:
                                        ident = ident.split("fst")[-1]
                                    ident = ident.lstrip("0")

                                    self.out_cache[ident] = {"xml": etree.tostring(record, encoding="unicode")}
                                    records_processed += 1
                                else:
                                    print(f"FAST record in {fn} is missing an identifier.")

                        except etree.XMLSyntaxError as e:
                            print(f"Error parsing XML in {fn}: {e}")

            if records_processed > 0:
                self.out_cache.commit()
                print(f"Successfully loaded {records_processed} records into cache.")
            else:
                print("No valid records were processed.")

        except (zipfile.BadZipFile, FileNotFoundError) as e:
            print(f"Failed to open ZIP file {self.in_path}: {e}")
