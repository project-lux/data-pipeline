{
    "name": "geonames",
    "type": "external",
    "namespace": "https://sws.geonames.org/",
    "matches": ["geonames.org/"],
    "fetch": "https://sws.geonames.org/{identifier}/about.rdf",
    "wikidata_property": ["P1566"],
    "merge_order": 80,
    
    "fetcherClass": "sources.general.geonames.fetcher.GnFetcher",
    "mapperClass": "sources.general.geonames.mapper.GnMapper",
    "loaderClass": "sources.general.geonames.loader.GnLoader",

    "dumpFilePath": "geonames",
    "cycleBreakPath": "geonames_cycle_breaks.json",

    "dump_file_subdir": "geonames",
    "remote_dump_files": [
        "https://download.geonames.org/export/dump/allCountries.zip", 
        "https://download.geonames.org/export/dump/alternateNamesV2.zip",
        "https://download.geonames.org/export/dump/hierarchy.zip"
    ]
}
