# from lux_pipeline.process.base.mapper import Mapper
from lux_pipeline.process.base.mapper import Mapper
from cromulent import model, vocab
import requests
import re

class PleiadesMapper(Mapper):
    def __init__(self, config):
        Mapper.__init__(self, config)
        self.factory.auto_assign_id = False
        self.place_type_map = self._load_place_types()
    
    def _load_place_types(self):
        """Download and parse the Pleiades place-types.ttl file to build the mapping."""
        url = "https://atlantides.org/downloads/pleiades/rdf/place-types.ttl"
        try:
            response = requests.get(url)
            response.raise_for_status()
            content = response.text
            
            # Parse the TTL content
            place_types = {}
            current_type = None
            current_aat = None
            
            for line in content.split('\n'):
                # Look for place type definitions
                type_match = re.search(r'<https://pleiades\.stoa\.org/vocabularies/([^>]+)>', line)
                if type_match:
                    current_type = type_match.group(1)
                
                # Look for AAT URIs
                aat_match = re.search(r'<http://vocab\.getty\.edu/aat/([^>]+)>', line)
                if aat_match and current_type:
                    current_aat = f"http://vocab.getty.edu/aat/{aat_match.group(1)}"
                    place_types[current_type] = current_aat
            
            return place_types
        except Exception as e:
            print(f"Warning: Could not load place types from {url}: {str(e)}")
            return {}
    
    def guess_type(self, data):
        # Check if it's a place type concept
        if "placeTypeURIs" in data and data["placeTypeURIs"]:
            return model.Type
        return model.Place

    def transform(self, record, rectype, reference=False):
        rec = record["data"]
        recid = record["identifier"]

        # Handle place type concepts
        if rectype == "Type" or (not rectype and "placeTypeURIs" in rec):
            top = model.Type(ident=rec["uri"])
            if "title" in rec:
                top.label = rec["title"]
            if "description" in rec:
                desc = vocab.Description(content=rec["description"])
                desc.language = model.Language(ident="http://vocab.getty.edu/aat/300388277")  # English
                top.referred_to_by = desc
            return {"identifier": recid, "data": model.factory.toJSON(top), "source": "pleiades"}

        # Handle places
        top = model.Place(ident=rec["uri"])
        
        # Add primary name from title
        if "title" in rec:
            primary_name = vocab.PrimaryName(content=rec["title"])
            # Add English language if not specified
            primary_name.language = model.Language(ident="http://vocab.getty.edu/aat/300388277")  # English
            top.identified_by = primary_name

        # Add description
        if "description" in rec:
            desc = vocab.Description(content=rec["description"])
            # Add English language
            desc.language = model.Language(ident="http://vocab.getty.edu/aat/300388277")  # English
            top.referred_to_by = desc

        # Add place types with AAT mappings
        if "placeTypes" in rec and rec["placeTypes"]:
            for pt in rec["placeTypes"]:
                if pt in self.place_type_map:
                    aat_uri = self.place_type_map[pt]
                    top.classified_as = model.Type(ident=aat_uri)
                else:
                    top.classified_as = model.Type(label=pt)

        # Add location from reprPoint
        if "reprPoint" in rec and rec["reprPoint"]:
            coords = rec["reprPoint"]
            if len(coords) >= 2:
                top.defined_by = {
                    "type": "Point",
                    "coordinates": [coords[0], coords[1]]
                }

        # Add attested names and languages
        if "names" in rec and rec["names"]:
            for n in rec["names"]:
                if "attested" in n:
                    nm = model.Name(content=n["attested"])
                    # Classify as alternate name if not the primary name
                    if n["attested"] != rec.get("title"):
                        nm.classified_as = model.Type(ident="http://vocab.getty.edu/aat/300264273")  # Alternate Name
                    if "language" in n:
                        nm.language = model.Language(label=n["language"])
                    top.identified_by = nm

        # Add part_of relationships
        if "connections" in rec and rec["connections"]:
            for conn in rec["connections"]:
                if conn.get("connectionType") == "part_of_physical":
                    related = model.Place(ident=conn["connectsTo"])
                    if "title" in conn:
                        related.label = conn["title"]
                    top.part_of = related

        data = model.factory.toJSON(top)
        return {"identifier": recid, "data": data, "source": "pleiades"}