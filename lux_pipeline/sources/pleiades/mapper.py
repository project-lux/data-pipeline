# from lux_pipeline.process.base.mapper import Mapper
from lux_pipeline.process.base.mapper import Mapper
from cromulent import model, vocab
import re

class PleiadesMapper(Mapper):
    def __init__(self, config):
        Mapper.__init__(self, config)
        self.factory.auto_assign_id = False
    
    def guess_type(self, data):
        # Check the @type field first
        if data.get("@type") == "Concept":
            return model.Type
        # Default to Place for everything else, including records with placeTypeURIs
        return model.Place

    def geojson_to_wkt(self, geom):
        """Convert a GeoJSON geometry dict to a WKT string."""
        t = geom.get("type")
        coords = geom.get("coordinates")
        if t == "Point":
            return f"POINT ({coords[0]} {coords[1]})"
        elif t == "Polygon":
            # Polygon: list of linear rings (first is exterior)
            rings = []
            for ring in coords:
                rings.append(", ".join(f"{x} {y}" for x, y in ring))
            return f"POLYGON (({rings[0]}))"
        elif t == "MultiPolygon":
            polys = []
            for poly in coords:
                rings = []
                for ring in poly:
                    rings.append(", ".join(f"{x} {y}" for x, y in ring))
                polys.append(f"(({rings[0]}))")
            return f"MULTIPOLYGON ({', '.join(polys)})"
        # Add more types as needed
        return None

    def bbox_to_wkt(self, bbox):
        """Convert a bounding box [minLon, minLat, maxLon, maxLat] to a WKT Polygon string."""
        minx, miny, maxx, maxy = bbox
        # Polygon: lower left, lower right, upper right, upper left, close
        return (
            f"POLYGON (({minx} {miny}, {maxx} {miny}, {maxx} {maxy}, {minx} {maxy}, {minx} {miny}))"
        )
    
    def parse_types(self, ttl_section):
        """Parse a TTL section for a place type concept and create a Linked Art record."""
        # Extract the URI from the first line
        uri_match = re.search(r'<https://pleiades\.stoa\.org/vocabularies/([^>]+)>', ttl_section)
        if not uri_match:
            return None
        
        concept_id = uri_match.group(1)
        uri = f"https://pleiades.stoa.org/vocabularies/{concept_id}"
        
        # Extract prefLabel
        label_match = re.search(r'skos:prefLabel "([^"]+)"@en', ttl_section)
        if not label_match:
            return None
        
        label = label_match.group(1)
        
        # Extract scopeNote (description)
        scope_match = re.search(r'skos:scopeNote "([^"]+)"@en', ttl_section)
        description = scope_match.group(1) if scope_match else None
        
        # Extract AAT URI if present
        aat_match = re.search(r'<http://vocab\.getty\.edu/aat/([^>]+)>', ttl_section)
        aat_uri = f"http://vocab.getty.edu/aat/{aat_match.group(1)}" if aat_match else None
        
        # Create the Type record
        top = model.Type(ident=uri)
        
        # Add primary name
        primary_name = vocab.PrimaryName(content=label)
        primary_name.language = model.Language(ident="http://vocab.getty.edu/aat/300388277")  # English
        top.identified_by = primary_name
        
        # Add description if present
        if description:
            desc = vocab.Description(content=description)
            desc.language = model.Language(ident="http://vocab.getty.edu/aat/300388277")  # English
            top.referred_to_by = desc
        
        # Add AAT equivalent if present
        if aat_uri:
            top.equivalent = model.Type(ident=aat_uri)
        
        data = model.factory.toJSON(top)
        return {"identifier": concept_id, "data": data, "source": "pleiades"}

    def parse_place(self, record):
        rec = record["data"]
        recid = record["identifier"]

        # Handle places
        top = model.Place(ident=rec["uri"])
        # Add primary name from title or names
        title = rec.get("title")
        primary_name = None
        if title:
            primary_name = vocab.PrimaryName(content=title)
            primary_name.language = model.Language(ident="http://vocab.getty.edu/aat/300388277")  # English
        else:
            # Try to find a name in names
            names = rec.get("names", [])
            for n in names:
                if "attested" in n and n["attested"]:
                    primary_name = vocab.PrimaryName(content=n["attested"])
                    if "language" in n:
                        primary_name.language = model.Language(label=n["language"])
                    else:
                        primary_name.language = model.Language(ident="http://vocab.getty.edu/aat/300388277")
                    break
        if not primary_name:
            return None
        top.identified_by = primary_name

        # Add description
        if "description" in rec:
            desc = vocab.Description(content=rec["description"])
            # Add English language
            desc.language = model.Language(ident="http://vocab.getty.edu/aat/300388277")  # English
            top.referred_to_by = desc

        # Add place types using placeTypeURIs directly
        if "placeTypeURIs" in rec:
            for pt_uri in rec["placeTypeURIs"]:
                top.classified_as = model.Type(ident=pt_uri)

        # Add geospatial data: geometry, bbox, reprPoint (in that order)
        wkt = None
        if "geometry" in rec and rec["geometry"]:
            wkt = self.geojson_to_wkt(rec["geometry"])
        elif "bbox" in rec and rec["bbox"]:
            wkt = self.bbox_to_wkt(rec["bbox"])
        elif "boundingBox" in rec and rec["boundingBox"]:
            wkt = self.bbox_to_wkt(rec["boundingBox"])
        elif "reprPoint" in rec and rec["reprPoint"]:
            coords = rec["reprPoint"]
            if len(coords) >= 2:
                wkt = f"POINT ({coords[0]} {coords[1]})"
        if wkt:
            top.defined_by = wkt

        # Add part_of relationships
        if "connections" in rec and rec["connections"]:
            for conn in rec["connections"]:
                if conn.get("connectionType") == "succeeds":
                    related = model.Place(ident=conn["connectsTo"])
                    if "title" in conn:
                        related._label = conn["title"]
                    top.part_of = related
        if "references" in rec and rec["references"]:
            for ref in rec["references"]:
                if "https://www.wikidata.org/wiki" in ref["accessURI"]:
                    top.equivalent = model.Place(ident=ref["accessURI"])
        data = model.factory.toJSON(top)
        return {"identifier": recid, "data": data, "source": "pleiades"}

    def transform(self, record, rectype, reference=False):

        if rectype == "Place":
            return self.parse_place(record)
        elif rectype == "Type":
            return self.parse_concept(record)
        else:
            return None
