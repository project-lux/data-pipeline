
import os
import json

from jsonschema import validate, Draft202012Validator
from jsonschema.exceptions import ValidationError
from jsonschema._utils import find_additional_properties
from referencing import Registry, Resource

class Validator(object):

    def __init__(self, configs):

        # This will let local extension properties starting with _ pass validation
        # e.g. _content_html and _seconds_since_epoch_begin_of_the_begin
        self.underscore_okay = configs.validate_allow_underscore_properties

        # Pick up files from DATA/schema/*
        sdir = os.path.join(configs.data_dir, "schema")
        self.schema_map = {
            'DigitalObject': 'digital',
            'Group': 'group',
            'VisualItem': 'image',
            'HumanMadeObject': 'object',
            'Person': 'person',
            'Place': 'place',
            'Set': 'set',
            'LinguisticObject': 'text',
            'Type': 'concept',
            'Activity': 'provenance',
            'Event': 'event'
        }        

        # first read in and register core
        corefn = os.path.join(sdir, f"core.json")
        fh = open(corefn)
        core_json = json.load(fh)
        fh.close()

        schema = Resource.from_contents(core_json)
        registry = Registry().with_resources([
                ("https://linked.art/api/1.0/schema/core.json", schema),
                ("core.json", schema)])

        for (k,v) in self.schema_map.items():
            fh = open(os.path.join(sdir, f"{v}.json"))
            schema = json.load(fh)
            fh.close()
            vldr = Draft202012Validator(schema, registry=registry)
            self.schema_map[k] = vldr
        self.schema_map['Period'] = self.schema_map['Event']
        self.schema_map['Material'] = self.schema_map['Type']
        self.schema_map['Currency'] = self.schema_map['Type']
        self.schema_map['Language'] = self.schema_map['Type']
        self.schema_map['MeasurementUnit'] = self.schema_map['Type']

    def validate(self, rec):

        # Find out which type of record we are
        js = rec['data']
        if not js['type']:
            # Fundamentally broken
            raise ValueError(f"Record does not have a class!")
        elif not js['type'] in self.schema_map:
            raise ValueError(f"Cannot validate record with class: {js['type']}")
        else:
            validator = self.schema_map[js['type']]

        # give the json to the validator
        errs = []
        # Use with: f"  /{'/'.join([str(x) for x in error.absolute_path])} --> {error.message} "
        for error in validator.iter_errors(js):
            if self.underscore_okay and error.validator == 'additionalProperties':
                aps = []
                for ap in find_additional_properties(error.instance, error.schema):
                    if ap[0] != '_':
                        aps.append(ap)
                if not aps:
                    continue
            errs.append(error)

        return errs
