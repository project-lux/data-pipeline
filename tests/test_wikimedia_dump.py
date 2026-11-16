"""Wikimedia Commons from a MediaWiki XML dump: WmLoader keeps the File
pages that are images with a PD, CC0, CC BY 4.0 or CC BY-SA 4.0 licence,
keyed as the image lookup in lux.final.mapper looks them up, and WmMapper
maps them as it maps the API records the fetcher stores."""

import bz2
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from xml.sax.saxutils import escape

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.config import importObject
from lux_pipeline.sources.wikimedia import wikitext as wt
from lux_pipeline.sources.wikimedia.loader import WmLoader, file_key
from lux_pipeline.sources.wikimedia.mapper import WmMapper

INFO = """=={{{{int:filedesc}}}}==
{{{{Information
|description={{{{sr|1=Опшина Рашка}}}}
{{{{en|1=Raska Municipality - '''Banja''' [[Jošanička Banja|Jošanička]]<br/>spa}}}}
|date=2016-08-21
|source={{{{own}}}}
|author=[[User:Olja Simović|Olja Simović]]
}}}}

=={{{{int:license-header}}}}==
{lic}
<!-- {{{{cc-by-sa-4.0}}}} in a comment grants nothing -->
"""


def page(title, text, ns=6, pageid=1, redirect=False):
    red = f'<redirect title="{title}x" />' if redirect else ""
    return (f"<page><title>{escape(title)}</title><ns>{ns}</ns><id>{pageid}</id>{red}"
            f"<revision><id>9{pageid}</id><timestamp>2026-04-07T04:49:38Z</timestamp>"
            f'<text bytes="1" xml:space="preserve">{escape(text)}</text></revision></page>')


PAGES = [
    page("File:Izvori vruće vode 0114.jpg", INFO.format(lic="{{self|cc-by-sa-4.0}}"), pageid=1),
    page("File:Old.jpg", INFO.format(lic="{{self|cc-by-sa-3.0}}"), pageid=2),
    page("File:Painting.tif", INFO.format(lic="{{Licensed-PD-Art|1=PD-old-auto|2=PD-x}}"), pageid=3),
    page("File:Talk.ogg", INFO.format(lic="{{cc-zero}}"), pageid=4),
    page("File:Moved.jpg", "#REDIRECT [[File:Other.jpg]]", pageid=5, redirect=True),
    page("Category:Spas", "{{cc-zero}}", ns=14, pageid=6),
    page("File:Doubtful.png", INFO.format(lic="{{cc-zero}}{{copyvio|reason}}"), pageid=7),
    page("File:Dual.PNG", INFO.format(lic="{{self|GFDL|cc-by-4.0|migration=redundant}}"), pageid=8),
]


def dump(tmp_path, pages=PAGES):
    xml = ('<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.11/"><siteinfo><sitename>Wikimedia '
           'Commons</sitename></siteinfo>' + "".join(pages) + "</mediawiki>")
    path = tmp_path / "commonswiki-p1p8.xml.bz2"
    path.write_bytes(bz2.compress(xml.encode()))
    return path


class Cache(dict):
    def checkpoint(self):
        pass

    def commit(self):
        pass


def load(tmp_path, path, type_="mediawiki.bz2/raw", slices=(0, 1), **keys):
    cache = Cache()
    cfg = {"name": "wikimedia", "datacache": cache, "mapper": None,
           "all_configs": SimpleNamespace(dumps_dir=str(tmp_path), temp_dir=str(tmp_path), data_dir=str(tmp_path)),
           "namespace": "http://commons.wikimedia.org/",
           "input_files": {"records": [{"path": str(path), "type": type_}]}, **keys}
    ldr = WmLoader(cfg)
    ldr.prepare(*slices)
    ldr.process()
    return ldr, cache


def mapper():
    m = object.__new__(WmMapper)
    m.name, m.licenses = "wikimedia", ["pd", "cc0", "cc-by-sa-4.0", "cc-by-4.0"]
    return m


# --- licences --------------------------------------------------------------------

@pytest.mark.parametrize("text, codes", [
    ("{{self|cc-by-sa-4.0}}", ["cc-by-sa-4.0"]),
    ("{{Self|Cc-by-sa-4.0,3.0,2.5,2.0,1.0}}", ["cc-by-sa-4.0"]),
    ("{{cc-by-4.0|[http://www.kremlin.ru Kremlin.ru]}}", ["cc-by-4.0"]),
    ("{{self|GFDL|cc-by-sa-4.0|author=X}}", ["cc-by-sa-4.0"]),
    ("{{CC-zero}}", ["cc0"]), ("{{self|Cc-zero}}", ["cc0"]), ("{{CC0}}", ["cc0"]),
    ("{{PD-self}}", ["pd"]), ("{{PD-old-70-1923}}", ["pd"]), ("{{PD-USGov-Military-Army}}", ["pd"]),
    ("{{PD-Art|PD-old-100}}", ["pd"]), ("{{Licensed-PD-Art|1=PD-old-auto|2=x}}", ["pd"]),
    ("|permission = {{PD-FLGov-PhotoColl}}", ["pd"]), ("{{cc-by-sa-all}}", ["cc-by-sa-4.0"]),
    ("{{PD-self}}{{self|cc-by-4.0}}{{cc-zero}}", ["pd", "cc0", "cc-by-4.0"]),
    ("{{cc-by-sa-3.0}}", []), ("{{cc-by-sa 3.0|Berthold Werner}}", []), ("{{cc-by-3.0-de}}", []),
    ("{{self|GFDL}}", []), ("{{cc-by-sa-3.0-migrated}}", []), ("{{YouTube CC-BY}}", []),
    ("{{User:FlickreviewR/reviewed-pass|x|https://flickr.com/}}", []),
    ("{{PD-self}}{{delete|reason=copyvio}}", []), ("{{cc-zero}}{{No permission since|month=May}}", []),
    ("<!-- {{cc-zero}} -->", []), ("", []),
])
def test_licences(text, codes):
    assert wt.page_licenses(text) == codes


def test_allowed_licences_narrow_the_codes():
    assert wt.page_licenses("{{PD-self}}{{cc-by-sa-4.0}}", allowed=("cc-by-sa-4.0",)) == ["cc-by-sa-4.0"]


def test_nested_templates_and_arguments():
    [outer, inner] = wt.templates("{{Information|description={{en|1=a|b}}|author=[[User:X|X]]}}")
    assert outer.name == "information" and outer.named["author"] == "[[User:X|X]]"
    assert (inner.name, inner.named, inner.positional) == ("en", {"1": "a"}, ["b"])


def test_fields_as_plain_text():
    f = wt.file_fields(INFO.format(lic=""))
    assert f == {"description": "Raska Municipality - Banja Jošanička spa", "author": "Olja Simović",
                 "source": "Own work", "date": "2016-08-21"}
    assert wt.render("{{fr|1=Bonjour}}{{de|1=Hallo}}") == "Bonjour"
    assert wt.render("{{Creator:Jan Soreau}} [https://x.org/y the site] [https://x.org/z] &amp;") == "Jan Soreau the site &"


# --- the loader --------------------------------------------------------------------

def test_only_licensed_image_file_pages_are_loaded(tmp_path):
    ldr, cache = load(tmp_path, dump(tmp_path))
    assert sorted(cache) == ["Dual.PNG", "Izvori_vru%C4%87e_vode_0114.jpg", "Painting.tif"]
    rec = cache["Izvori_vru%C4%87e_vode_0114.jpg"]
    assert (rec["pageid"], rec["revision"], rec["license"], rec["licenses"]) == (1, "91", "cc-by-sa-4.0", ["cc-by-sa-4.0"])
    assert rec["title"] == "File:Izvori vruće vode 0114.jpg" and "{{self|cc-by-sa-4.0}}" in rec["wikitext"]
    assert cache["Painting.tif"]["license"] == "pd" and cache["Dual.PNG"]["license"] == "cc-by-4.0"
    assert ldr.pages["file"] == 6 and ldr.pages["redirect"] == 1 and ldr.pages["other"] == 1
    assert ldr.pages["not an image"] == 1 and ldr.pages["no allowed licence"] == 2
    assert ldr.pages["kept"] == 3


def test_extensions_and_licences_come_from_the_config(tmp_path):
    _, cache = load(tmp_path, dump(tmp_path), image_extensions=["jpg", "ogg"], allowed_licenses=["cc0", "cc-by-sa-4.0"])
    assert sorted(cache) == ["Izvori_vru%C4%87e_vode_0114.jpg", "Talk.ogg"]


def test_workers_share_the_pages(tmp_path):
    path = dump(tmp_path)
    got = [sorted(load(tmp_path, path, slices=(n, 2))[1]) for n in range(2)]
    assert sorted(got[0] + got[1]) == ["Dual.PNG", "Izvori_vru%C4%87e_vode_0114.jpg", "Painting.tif"]
    assert got[0] and got[1]


def test_a_directory_of_dump_parts_sliced_by_file(tmp_path):
    parts = tmp_path / "commonswiki"
    parts.mkdir()
    (parts / "a.xml.bz2").write_bytes(dump(tmp_path, PAGES[:2]).read_bytes())
    (parts / "b.xml.bz2").write_bytes(dump(tmp_path, PAGES[2:]).read_bytes())
    cfg_type = "dir/mediawiki.bz2/raw"
    _, cache = load(tmp_path, parts, type_=cfg_type)
    assert sorted(cache) == ["Dual.PNG", "Izvori_vru%C4%87e_vode_0114.jpg", "Painting.tif"]


def test_a_file_given_on_the_command_line_is_read_as_a_dump(tmp_path):
    path = dump(tmp_path)
    ldr, _ = load(tmp_path, path)
    assert ldr.sniffed_type(str(path)) == "mediawiki.bz2/raw"
    plain = tmp_path / "part.xml"
    plain.write_bytes(bz2.decompress(path.read_bytes()))
    assert ldr.sniffed_type(str(plain)) == "mediawiki/raw"
    # lux load --file: the configured entry is a directory of parts
    cache = Cache()
    cfg = {"name": "wikimedia", "datacache": cache, "mapper": None, "namespace": "http://commons.wikimedia.org/",
           "all_configs": SimpleNamespace(dumps_dir=str(tmp_path), temp_dir=str(tmp_path), data_dir=str(tmp_path)),
           "input_files": {"records": [{"path": "commonswiki", "type": "dir/mediawiki.bz2/raw", "slice": "files"}]}}
    ldr = WmLoader(cfg)
    ldr.prepare(0, 1, path=str(path))
    ldr.process()
    assert len(cache) == 3


def test_the_key_is_the_image_lookups():
    # lux.final.mapper: the Special:FilePath name, %20 as _, then quote()
    assert file_key("File:Novinský potok (3).jpg") == "Novinsk%C3%BD_potok_%283%29.jpg"


# --- the mapper --------------------------------------------------------------------

def test_a_dump_page_maps_like_an_api_record(tmp_path):
    _, cache = load(tmp_path, dump(tmp_path))
    key = "Izvori_vru%C4%87e_vode_0114.jpg"
    out = mapper().transform({"identifier": key, "data": cache[key]}, "")["data"]
    assert out["access_point"][0]["id"] == "https://commons.wikimedia.org/wiki/Special:Filepath/Izvori vruće vode 0114.jpg"
    assert [n["content"] for n in out["identified_by"]] == ["Izvori vruće vode 0114"]
    [desc, rights] = [r["content"] for r in out["referred_to_by"]]
    assert desc == "Raska Municipality - Banja Jošanička spa"
    assert rights == ('<span>Izvori vruće vode 0114 by Olja Simović Own work, '
                      '<a href="https://creativecommons.org/licenses/by-sa/4.0">CC BY-SA 4.0</a></span>')
    assert out["subject_to"][0]["classified_as"][0]["id"] == "https://creativecommons.org/licenses/by-sa/4.0"
    # a TIFF is loaded but, as from the API, not shown
    assert mapper().transform({"identifier": "Painting.tif", "data": cache["Painting.tif"]}, "") is None


def test_an_api_record_still_maps():
    api = {"query": {"pages": {"1": {"title": "File:A b.jpg", "imageinfo": [{"extmetadata": {
        "License": {"value": "cc0"}, "ObjectName": {"value": "A <b>b</b>"},
        "ImageDescription": {"value": "<p>Desc</p>"}, "LicenseUrl": {"value": "http://cc.org/zero"},
        "UsageTerms": {"value": "CC Zero"}, "LicenseShortName": {"value": "CC0"},
        "Artist": {"value": "<a>Me</a>"}, "Credit": {"value": "Own work"}}}]}}}}
    rec = {"identifier": "A_b.jpg", "data": api}
    out = mapper().transform(rec, "")["data"]
    assert [n["content"] for n in out["identified_by"]] == ["A b"]
    assert [r["content"] for r in out["referred_to_by"]] == [
        "Desc", '<span>A b by <a>Me</a> Own work, <a href="http://cc.org/zero">CC0</a></span>']
    assert rec["data"]["query"]["pages"]   # no longer emptied by mapping
    api["query"]["pages"]["1"]["imageinfo"][0]["extmetadata"]["License"]["value"] = "cc-by-sa-3.0"
    assert mapper().transform(rec, "") is None


def test_the_class_resolves():
    assert importObject("sources.wikimedia.loader.WmLoader") is WmLoader
