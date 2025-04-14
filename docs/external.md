# External Data Sources

## Implementation Status

| Source          | Fetch | Map | Name Reconcile | Load | IndexLoad | ActivityStream |
| --------------- | ----- | --- | -------------- | ---- | ------- | -------------- |
| AAT*^           |   ✅  |  ✅ |    ✅         | N/A | ✅      | ✅ |
| DNB             |   ✅  |  ✅ |     -         |  ✅ | N/A       | N/A |
| FAST            |   ✅  |  -  |     -          | ✅  | N/A     | N/A |
| Geonames        |   ✅  |  ✅ |     -          | ✅  | N/A      | N/A |
| LCNAF *^        |   ✅  |  ✅ |     ✅        |  ✅ | ✅       | ✅ |
| LCSH *^         |   ✅  |  ✅ |     ✅        |  ✅ | ✅      | ✅ |
| TGN             |   ✅  |  ✅ |     -         | N/A  | N/A      | ✅ |
| ULAN *^         |   ✅  |  ✅ |     ✅        | N/A  | ✅      | ✅ |
| VIAF ^          |   ✅  |  ✅ |     -         |  ✅  | ✅       | N/A |
| Who's on First  |   ✅  |  ✅ |     -         | N/A  | N/A       | N/A |
| Wikidata ^      |   ✅  |  ✅ |     -         |  ✅  | ✅     | N/A |
| Japan NDL        |   ✅  |  ✅ |     -         | N/A  | N/A       | N/A |
| BNF             |   ✅  |  ✅ |     -         | ✅  | N/A       | N/A |
| GBIF            |   ✅  |  ✅ |     -         | N/A  | N/A       | N/A |
| ORCID           |   ✅  |  ✅ |     -         | ✅  | N/A       | N/A |
| ROR             |   ✅  |  ✅ |     -         | ✅  | N/A       | N/A |
| Wikimedia       |   ✅  |  ✅ |     -         | N/A  | N/A       | N/A |
| DNB             |   ✅  |  ✅ |     -         | ✅  | N/A       | N/A |
| BNE             |   ✅  |  ✅ |     -         | ✅  | N/A       | N/A |
| SNAC            |   ✅  |  ✅ |     -         | N/A  | N/A       | N/A |
| Homosaurus      |   ✅  |  ✅ |     -          | ✅ | N/A      | N/A |
| Nomisma         |   ✅  |  ✅ |     -          | ✅ | N/A      | N/A |




✅ = Done ; - = Not started ; N/A = Can't/Won't be done


### Notes:
- Indicates name is indexed: `*`
- Indicates URI is indexed: `^`

---

## External Source Details

- **Getty Vocabularies**: Authoritative, structured thesauri and union list used for cataloging, research, and interoperability in art, architecture, and cultural heritage domains.
  - ActivityStreams are updated monthly.
  - LUX harvests the following datasets, all available via the [main vocab AS](https://data.getty.edu/vocab/activity-stream/).
    - AAT (Art & Architecture Thesaurus) 
      - Linked.art Class: Concept 
    - TGN (Thesaurus of Geographic Names) 
      - Linked.art Class: Place 
    - ULAN (Union List of Artist Names) 
      - Linked.art Class: Person, Group
  - Format: JSON-LD

  - Individual records can be fetched at (e.g.):
    `https://vocab.getty.edu/aat/{identifier}.jsonld`

- **DNB (German National Library)**: A comprehensive repository of bibliographic and authority data for German-speaking regions.   
  - Dump files are updated monthly.
  - LUX harvests the following datasets:
    - [Sachbegriff](https://data.dnb.de/opendata/authorities-gnd-sachbegriff_lds.jsonld.gz)
      - Linked.art Class: Concept, Group
    - [Entity Facts](https://data.dnb.de/opendata/authorities-gnd_entityfacts.jsonld.gz)
      - Linked.art Class: Person, Group, Place, Event
    - [Mapped Authorities](https://data.dnb.de/opendata/mapping-authorities-gnd-lcsh-ram_lds.jsonld.gz)
      - Linked.art Property: equivalent

  - Format: JSON-LD
  
  - Individual records can be fetched at:
    `https://hub.culturegraph.org/entityfacts/{identifier}`

- **Geonames**: Geographical database that provides data on over 25 million places worldwide, including names, coordinates, and other metadata.
  - Dump files are updated daily.
  - LUX harvests the following datasets:
    - [All Countries](https://download.geonames.org/export/dump/allCountries.zip)
      - Linked.art Class: Place
    - [Alternate Names V2](https://download.geonames.org/export/dump/alternateNamesV2.zip)
      - Linked.art Property: identifiedBy
    - [Hierarchy](https://download.geonames.org/export/dump/hierarchy.zip)
      - Linked.art Property: partOf

  - Format: CSV/RDF 

  - Individual records can be fetched at:
      `https://sws.geonames.org/{identifier}/about.rdf`

- **FAST (Faceted Application of Subject Terminology)**:  Simplified subject vocabulary derived from the Library of Congress Subject Headings (LCSH).
  - Dump files do not have a specified update frequency, but the webpage includes the upload date for each dataset.
  - LUX harvests the following dataset:
    - [FAST ALL](https://researchworks.oclc.org/researchdata/fast/FASTAll.marcxml.zip)
      - Linked.art Class: *not yet mapped*

  - Format: MARC-XML 
  
  - Individual records can be fetched at:
      `https://id.worldcat.org/fast/{identifier}.rdf.xml`

- **Wikidata**: Collaborative, multilingual, and structured knowledge base that stores linked data to support Wikimedia projects and beyond.
  - Dump file updated weekly, typically on Mondays. 
  - LUX harvests the following dataset:
    - [Wikidata Latest All](https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz)
      - Linked.art Class: HumanMadeObject, LinguisticObject, Person, Group, Concept, Place, Event

  - Format: JSON-LD

  - Individual records can be fetched at:
      `https://www.wikidata.org/wiki/Special:EntityData/{identifier}.json`

- **Wikimedia Commons**: Free, collaborative media repository that hosts millions of openly licensed images, videos, audio files, and other media.
  - No dump file available. LUX fetches images as they are referenced. LUX only fetches images with the following licenses:
    - pd, cc0, cc-by-sa-4.0, cc-by-4.0
    - Linked.art Class: Digital Object, related to their representational Class via the `representation` property.

  - Format: LUX only fetches images in .jpg, .jpeg, .gif and .png format. More are available via Wikimedia Commons.

  - Individual images can be fetched at:
      `https://en.wikipedia.org/w/api.php?action=query&prop=imageinfo&iiprop=extmetadata&titles=File:{identifier}&format=json`


- **Library of Congress**: Structured, machine-readable representations of authoritative bibliographic, subject, and name data.
  - Dump files do not have a specified update frequency.
  - LUX harvests the following datasets:
    - [NAF (Name Authority File)](https://id.loc.gov/download/authorities/names.madsrdf.jsonld.gz)
      - Linked.art Class: Person, Group, Place, Activity, Period
    - [SH (Subject Headings)](https://id.loc.gov/download/authorities/subjects.madsrdf.jsonld.gz) 
      - Linked.art Class: Concept 
    - [DGT (Demographic Group Terms)](https://id.loc.gov/download/authorities/demographicTerms.madsrdf.jsonld.gz) 
      - Linked.art Class: Concept

  - As of 2025, LUX prefers the Activity Stream for NAF and SH.
    - `https://id.loc.gov/authorities/names/activitystreams/feed/1`
    - `https://id.loc.gov/authorities/subjects/activitystreams/feed/1`

  - Format: JSON-LD/MADS/RDF

  - Individual records can be fetched at (e.g.):
    `http://id.loc.gov/authorities/names/{identifier}.json`


- **ORCID**: Unique, persistent identifier system for researchers and scholars.
  - Dump file is updated yearly in October.
  - LUX harvests the following dataset:
    - [2024 Summaries](https://orcid.figshare.com/ndownloader/files/49560102)
      - Linked.art Class: Person

  - Format: XML

  - Individual records can be fetched via Orcid's API, but LUX relies solely on the dump file.

- **ROR (Research Organization Registry)**: Global, open, and community-driven registry of unique identifiers for research organizations.
  - Dump file is updated monthly.
  - LUX harvests the following dataset:
    - [ROR Data](https://zenodo.org/records/14429114/files/v1.58-2024-12-11-ror-data.zip)
      - Linked.art Class: Group

  - Format: JSON

  - Individual records can be fetched at:
    `https://api.ror.org/organizations/{identifier}`

- **VIAF (Virtual International Authority File)**: International service that consolidates and links authority data for names of people, organizations, and more, from libraries and cultural institutions worldwide.
  - Dump files are typically updated monthly. However, as of August 2024, updating is on pause while VIAF undergoes security and production environment improvements.
  - LUX harvests the following dataset:
    - [VIAF Clusters](https://viaf.org/viaf/data/viaf-20240804-clusters.xml.gz)
      - Linked.art Class: Person, Group, Place

  - Format: XML

  - Individual records can be fetched at:
    `https://viaf.org/viaf/{identifier}/viaf.xml`


- **Who’s on First (WOF)**: Open-source gazetteer and database of geographic places, providing unique identifiers and metadata for locations worldwide.
  - Dump files do not have a specified update frequency, but the webpage includes the upload date for each dataset.
  - LUX harvests the following dataset:
    - [WOF Global Latest](https://data.geocode.earth/wof/dist/sqlite/whosonfirst-data-admin-latest.db.bz2)
      - Linked.art Class: Place

  - Format: SQLite database

  - Individual records can be fetched at:
    `https://data.whosonfirst.org/{identifier}`

- **SNAC (Social Networks and Archival Context)**: Cooperative initiative to discover biographical and historical information about people, families, and organizations, connecting them through archival records.
  - No dump file available. LUX fetches records as they are referenced. 
      - Linked.art Class: Person, Group

  - Format: JSON

  - Individual records can be fetched at:
    `https://snaccooperative.org/download?arkid=http://n2t.net/ark:/99166/{identifier}&type=constellation_json`


- **GBIF (Global Biodiversity Information Facility)**: International network and data platform that provides open access to biodiversity data, enabling research on species distribution and ecosystems worldwide.
  - No dump file of the entire dataset is available. LUX fetches records as they are referenced, usually from Yale Peabody Museum taxonomic records.
    - Linked.art Class: Concept

  - Format: JSON

  - Individual records can be fetched at:
    `https://api.gbif.org/v1/species/{identifier}`

- **Homosaurus**: International LGBTQ+ linked data vocabulary that provides standardized terms to improve the discovery and organization of LGBTQ+ resources in libraries, archives, and other information systems.
  - Dump files do not have a specified update frequency, but the webpage includes the upload date for each dataset.
  - LUX harvests the following dataset:
    - [V3](https://homosaurus.org/v3.jsonld)
      - Linked.art Class: Concept

  - Format: JSON-LD

  - Individual records can be fetched at:
    `https://homosaurus.org/v3/{identifier}.jsonld`

- **Nomisma**: Collaborative project that provides a linked open data vocabulary and digital resource for numismatics, focusing on the study of coins, currency, and related objects.
  - Dump files are updated nightly.
  - LUX harvests the following dataset:
    - [Nomisma](https://nomisma.org/nomisma.org.jsonld)
      - Linked.art Class: Person, Group, Place, Concept

  - Format: JSON-LD

  - Individual records can be fetched at:
    `http://nomisma.org/id/{identifier}.jsonld`


- **BNE (Biblioteca Nacional de España)**: National Library of Spain, which provides access to Spain's cultural and historical heritage through its collection of books, manuscripts, maps, and digital resources.
  - Dump files do not have a specified update frequency, but the webpage includes the upload date for each dataset.
  - LUX harvests the following datasets:
    - [Entidad](https://www.bne.es/media/datosgob/catalogo-autoridades/entidad/entidad-JSON.zip)
      - Linked.art Class: Group
    - [Materia](https://www.bne.es/media/datosgob/catalogo-autoridades/materia/materia-JSON.zip)
      - Linked.art Class: Concept
    - [Geografico](https://www.bne.es/media/datosgob/catalogo-autoridades/geografico/geografico-JSON.zip)
      - Linked.art Class: Place
    - [Persona](https://www.bne.es/media/datosgob/catalogo-autoridades/persona/persona-JSON.zip)
      - Linked.art Class: Person

    - Format: JSON

    - Individual records can be fetched at:
      `https://datos.bne.es/resource/{identifier}.jsonld`

- **BNF (Bibliothèque nationale de France)**: National library of France, preserving and providing access to a vast collection of books, manuscripts, and cultural heritage materials.
  - In the past, LUX relied on the BNF's RDF/JSON-LD for harvesting, however this service has not been consistently available. As a result, we swapped to the XML dump files.
  - Dump files do not have a specified update frequency.
  - LUX harvests the following datasets:
    - [DataBNF Rameau NoSubjects](https://transfert.bnf.fr/link/c26ba50e-17c4-46fe-b6d8-8c2ad393f40e)
      - Linked.art Class: Concept
    - [DataBNF Person Authors](https://transfert.bnf.fr/link/c412f451-2bf2-45a7-b76b-a11d563c2a8a)
      - Linked.art Class: People
    - [DataBNF Org Authors](https://transfert.bnf.fr/link/2a2b3690-f642-4644-8615-9b50b59c84d9)
      - Linked.art Class: Group
    - [DataBNF Geos](https://transfert.bnf.fr/link/86ea06b4-2590-4d1c-8e1e-126eff24b535)
      - Linked.art Class: Place

    - Format: XML* see note about RDF/JSON-LD above

    - If service is available, individual records can be fetched at:
      `https://data.bnf.fr/ark:/12148/{identifier}.rdfjsonld`


- **Japan NDL (Japanese National Diet Library)**: Provides access to a wide range of bibliographic and authority data, enabling researchers and institutions to retrieve and utilize information from the NDL's extensive collections.
  - While dump files are available for subject headings, LUX retrieves records as referenced.

  - Format: JSON-LD

  - Individual records can be fetched at, e.g.:
    `https://id.ndl.go.jp/auth/ndlsh/{identifier}.json`

