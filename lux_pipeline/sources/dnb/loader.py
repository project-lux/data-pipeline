"""DNB (GND): two dumps loaded into one datacache, both listed as
``input_files`` records entries:

* entityfacts, ``arraylines.gz/json``: one record per line inside a JSON
  array; the key is the GND id, the last segment of ``@id``;
* the Sachbegriff (subject headings) ``_lds.jsonld.gz`` file, in its own
  ``sachbegriff.gz/json`` container: a JSON array whose items are
  records or lists of records. Each record whose ``@id`` is a GND id
  (and not the ``.../about`` node) is stored as ``{"list": [record]}``
  under that id, the shape the web fetcher returns for subjects, which
  is what the hand-written loader did.

List entityfacts first: with overwriting on, a GND id present in both
files ends up with its subject form, as it did when the hand-written
loader loaded subjects first and skipped ids already present.
"""

from lux_pipeline.process.base.loader import Loader

GND = "/d-nb.info/gnd/"


class DnbLoader(Loader):
    extra_containers = ("sachbegriff",)

    def _init_dsl(self):
        super()._init_dsl()
        self.step_functions["sachbegriff"] = self.iterate_sachbegriff

    def iterate_sachbegriff(self, path, comp, remaining):
        """The subject headings file: an array of records or of lists of
        records, each GND record wrapped as the web form ``{"list": [rec]}``."""
        for member in self.iterate_array(path, comp, remaining):
            items = member if isinstance(member, list) else [member]
            for item in items:
                if not isinstance(item, dict):
                    continue
                ident = item.get("@id", "")
                if GND in ident and not ident.endswith("about"):
                    yield {"list": [item]}

    def extract_identifier(self, data):
        if isinstance(data, dict) and "list" in data:
            data = data["list"][0]
        uri = data.get("@id", "") if isinstance(data, dict) else ""
        return uri.rsplit("/", 1)[-1] or None
