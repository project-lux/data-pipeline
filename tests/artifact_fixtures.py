"""Generators the artifact tests configure (tests/test_artifacts.py). Not a
test module: a record's generatorClass names these by module path, which
works because pytest puts tests/ on sys.path."""

from lux_pipeline.artifacts.base import ArtifactGenerator


class Lines(ArtifactGenerator):
    """Writes each of the record's files with `rows` lines (``<file> <n>``);
    `fail` raises after writing them."""

    required_keys = ("rows",)
    optional_keys = {"fail": False}

    def generate(self, out_dir):
        out = {}
        for rel in self.config["files"]:
            with open(self.out_path(out_dir, rel), "w") as fh:
                for n in range(self.config["rows"]):
                    fh.write(f"{rel} {n}\n")
            out[rel] = self.config["rows"]
        if self.option("fail"):
            raise RuntimeError("the generator failed")
        return out


class FromWikidata(Lines):
    needs_sources = ("wikidata",)
