
import os
import ujson as json

from jsonschema import validate, Draft202012Validator
from jsonschema.exceptions import ValidationError
from jsonschema._utils import find_additional_properties
from referencing import Registry, Resource

SCHEMA_BASE_URL = "https://linked.art/api/1.0/schema/"

# Linked Art class -> schema file (without .json) under <data_dir>/schema/
SCHEMA_FILES = {
    'DigitalObject': 'digital',
    'Group': 'group',
    'VisualItem': 'image',
    'HumanMadeObject': 'object',
    'Person': 'person',
    'Place': 'place',
    'Set': 'set',
    'LinguisticObject': 'text',
    'Type': 'concept',
    'Event': 'event',
}
SCHEMA_ALIASES = {'Period': 'Event', 'Activity': 'Event', 'Material': 'Type',
                  'Currency': 'Type', 'Language': 'Type', 'MeasurementUnit': 'Type'}


def schema_file_names():
    """``core.json`` and the per-class schema files the validator reads."""
    return ["core.json"] + [f"{v}.json" for v in SCHEMA_FILES.values()]


class Validator(object):
    """Validates mapped records against the Linked Art JSON schemas in
    ``<data_dir>/schema/`` (``core.json`` plus one file per class, from
    `SCHEMA_BASE_URL`; `lux initialize` fetches them). The schemas are
    read on first `validate`, not here: a `Config` is built by every
    command, on every machine, and must not depend on these files."""

    def __init__(self, configs):

        # This will let local extension properties starting with _ pass validation
        # e.g. _content_html and _seconds_since_epoch_begin_of_the_begin
        self.underscore_okay = configs.validate_allow_underscore_properties

        # Pick up files from DATA/schema/*
        self.schema_dir = os.path.join(configs.data_dir, "schema")
        self.schema_map = {}
        self._loaded = False

    def missing(self):
        """The schema files not present under ``schema_dir``."""
        return [fn for fn in schema_file_names() if not os.path.exists(os.path.join(self.schema_dir, fn))]

    def load(self):
        """Read and register the schemas; `FileNotFoundError` naming what is
        missing and where to get it."""
        if self._loaded:
            return
        missing = self.missing()
        if missing:
            raise FileNotFoundError(
                f"validator schemas missing from {self.schema_dir}: {', '.join(missing)} "
                f"(from {SCHEMA_BASE_URL}; `lux initialize` fetches them)")
        sdir = self.schema_dir
        # first read in and register core
        with open(os.path.join(sdir, "core.json")) as fh:
            core_json = json.load(fh)
        schema = Resource.from_contents(core_json)
        registry = Registry().with_resources([
                ("https://linked.art/api/1.0/schema/core.json", schema),
                ("core.json", schema)])

        schema_map = {}
        for (k, v) in SCHEMA_FILES.items():
            with open(os.path.join(sdir, f"{v}.json")) as fh:
                schema = json.load(fh)
            schema_map[k] = Draft202012Validator(schema, registry=registry)
        for alias, cls in SCHEMA_ALIASES.items():
            schema_map[alias] = schema_map[cls]
        self.schema_map = schema_map
        self._loaded = True

    def validate(self, rec):
        self.load()

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
