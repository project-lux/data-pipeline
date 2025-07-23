from pipeline.process.base.mapper import Mapper
from cromulent import model, vocab
import re

class ISNIMapper(Mapper):
    def __init__(self, config):
        Mapper.__init__(self, config)
        self.factory.auto_assign_id = False
    
    def guess_type(self, data):
        """Determine entity type from RDF type"""
        rdf_type = data.get("@type", [])
        if isinstance(rdf_type, str):
            rdf_type = [rdf_type]
        
        if "schema:Person" in rdf_type or "Person" in rdf_type:
            return "Person"
        elif "schema:Organization" in rdf_type or "Organization" in rdf_type:
            return "Group"
        return "Person"  # Default fallback
    
    def extract_isni_number(self, uri):
        """Extract 16-digit ISNI from URI"""
        if 'isni.org/isni/' in uri:
            return uri.split('/')[-1]
        return None
    
    def format_isni_display(self, isni):
        """Format ISNI for display: 0000 0000 0000 0000"""
        if len(isni) == 16:
            return f"{isni[:4]} {isni[4:8]} {isni[8:12]} {isni[12:16]}"
        return isni
    
    def parse_person(self, data):
        """Map ISNI person record to Linked Art Person"""
        uri = data.get('@id', '')
        isni_number = self.extract_isni_number(uri)
        if not isni_number:
            return None
        
        top = model.Person(ident=uri)
        top._label = f"ISNI {self.format_isni_display(isni_number)}"
        
        # Add ISNI as identifier
        isni_id = vocab.LocalNumber(content=self.format_isni_display(isni_number))
        assignment = model.AttributeAssignment()
        assignment.carried_out_by = model.Group(ident="https://isni.org/", _label="ISNI International Agency")
        isni_id.assigned_by = assignment
        top.identified_by = isni_id
        
        # Add names from schema:alternateName
        alt_names = data.get('schema:alternateName', [])
        if isinstance(alt_names, str):
            alt_names = [alt_names]
        
        if alt_names:
            # First name as primary
            primary_name = vocab.PrimaryName(content=alt_names[0])
            top.identified_by = primary_name
            top._label = alt_names[0]  # Update label to use actual name
            
            # Rest as alternate names
            for name in alt_names[1:]:
                alt_name = vocab.AlternateName(content=name)
                top.identified_by = alt_name
        
        # Add birth date if present
        birth_date = data.get('schema:birthDate')
        if birth_date:
            birth = model.Birth()
            birth.timespan = model.TimeSpan()
            birth.timespan.identified_by = vocab.DisplayName(content=str(birth_date))
            top.born = birth
        
        # Add death date if present
        death_date = data.get('schema:deathDate')
        if death_date:
            death = model.Death()
            death.timespan = model.TimeSpan()
            death.timespan.identified_by = vocab.DisplayName(content=str(death_date))
            top.died = death
        
        # Add external equivalents
        same_as = data.get('owl:sameAs', [])
        if isinstance(same_as, str):
            same_as = [same_as]
        for equiv_uri in same_as:
            if isinstance(equiv_uri, dict):
                equiv_uri = equiv_uri.get('@id', equiv_uri)
            top.equivalent = model.Person(ident=equiv_uri)
        
        # Add Library of Congress authority references
        lc_authorities = data.get('madsrdf:isIdentifiedByAuthority', [])
        if isinstance(lc_authorities, dict):
            lc_authorities = [lc_authorities]
        for auth in lc_authorities:
            if isinstance(auth, dict) and '@id' in auth:
                auth_uri = auth['@id']
                if 'loc.gov/authorities' in auth_uri:
                    top.equivalent = model.Person(ident=auth_uri)
        
        json_data = model.factory.toJSON(top)
        return {"identifier": isni_number, "data": json_data, "source": "isni"}
    
    def parse_organization(self, data):
        """Map ISNI organization record to Linked Art Group"""
        uri = data.get('@id', '')
        isni_number = self.extract_isni_number(uri)
        if not isni_number:
            return None
        
        top = model.Group(ident=uri)
        top._label = f"ISNI {self.format_isni_display(isni_number)}"
        
        # Add ISNI as identifier
        isni_id = vocab.LocalNumber(content=self.format_isni_display(isni_number))
        assignment = model.AttributeAssignment()
        assignment.carried_out_by = model.Group(ident="https://isni.org/", _label="ISNI International Agency")
        isni_id.assigned_by = assignment
        top.identified_by = isni_id
        
        # Add names from schema:alternateName
        alt_names = data.get('schema:alternateName', [])
        if isinstance(alt_names, str):
            alt_names = [alt_names]
        
        if alt_names:
            # First name as primary
            primary_name = vocab.PrimaryName(content=alt_names[0])
            top.identified_by = primary_name
            top._label = alt_names[0]  # Update label to use actual name
            
            # Rest as alternate names
            for name in alt_names[1:]:
                alt_name = vocab.AlternateName(content=name)
                top.identified_by = alt_name
        
        # Add formation date if present
        founding_date = data.get('schema:foundingDate')
        if founding_date:
            formation = model.Formation()
            formation.timespan = model.TimeSpan()
            formation.timespan.identified_by = vocab.DisplayName(content=str(founding_date))
            top.formed_by = formation
        
        # Add dissolution date if present
        dissolution_date = data.get('schema:dissolutionDate')
        if dissolution_date:
            dissolution = model.Dissolution()
            dissolution.timespan = model.TimeSpan()
            dissolution.timespan.identified_by = vocab.DisplayName(content=str(dissolution_date))
            top.dissolved_by = dissolution
        
        # Add external equivalents
        same_as = data.get('owl:sameAs', [])
        if isinstance(same_as, str):
            same_as = [same_as]
        for equiv_uri in same_as:
            if isinstance(equiv_uri, dict):
                equiv_uri = equiv_uri.get('@id', equiv_uri)
            top.equivalent = model.Group(ident=equiv_uri)
        
        # Add Library of Congress authority references
        lc_authorities = data.get('madsrdf:isIdentifiedByAuthority', [])
        if isinstance(lc_authorities, dict):
            lc_authorities = [lc_authorities]
        for auth in lc_authorities:
            if isinstance(auth, dict) and '@id' in auth:
                auth_uri = auth['@id']
                if 'loc.gov/authorities' in auth_uri:
                    top.equivalent = model.Group(ident=auth_uri)
        
        json_data = model.factory.toJSON(top)
        return {"identifier": isni_number, "data": json_data, "source": "isni"}
    
    def transform(self, record, rectype=None, reference=False):
        """Main transform method following the standard mapper pattern"""
        # Extract the actual data from the record
        data = record.get('data', record)
        
        if not rectype:
            rectype = self.guess_type(data)
        
        if rectype == "Person" or rectype == model.Person:
            return self.parse_person(data)
        elif rectype == "Group" or rectype == model.Group:
            return self.parse_organization(data)
        else:
            return None