import os
import sys
from dotenv import load_dotenv
from pipeline.config import Config
from cromulent.model import Place

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

wd_data = cfgs.external["wikidata"]["datacache"]

attrib_counts = {}


human = "Q5"
props = ["P21", "P569", "P570", "P19", "P20", "P734", "P735"]
# gender, dob, dod, pob, pod, fam name, given name

people = 0
for data in wd_data.iter_records():
    # Find definite people and count all props
    okay = 0
    if "P31" in data["data"] and "Q5" in data["data"]["P31"]:
        okay = 1
    else:
        ct = 0
        for p in props:
            if p in data["data"]:
                ct += 1
        if ct > 1:
            okay = 1
    if okay:
        people += 1
        for k in data["data"].keys():
            if k.startswith("P"):
                try:
                    attrib_counts[k] += 1
                except:
                    attrib_counts[k] = 1
        if "P31" in data["data"]:
            for p in data["data"]["P31"]:
                try:
                    attrib_counts[f"P31_{p}"] += 1
                except:
                    attrib_counts[f"P31_{p}"] = 1
        if not people % 1000:
            print(people)
        if people > 500000:
            break


# P31 (26360)       instance of
# P17 (26284)       country
# P625 (26238)      coordinate location
# P646 (25772)      freebase id
# P131 (25209)      located in the administrative territorial entity
# P373 (24660)      commons category
# P1566 (24120)     geonames id
# P18 (22987)       image
# P214 (21498)      viaf
# P910 (20246)      topic main category
# P2046 (19675)     area
# P1082 (18696)     population
# P6766 (18421)     WoF id
# P8189 (17967)     NL Israel
# P402 (17870)      OSM id
# P244 (17387)      LoC
# P856 (17300)      website
# P242 (16619)      locator map image
# P421 (15390)      in time zone
# P2044 (15125)     elevation above sea level
# P281 (13740)      postal code
# P227 (13136)      GND id
# P7859 (13052)     WorldCat id
# P47 (12293)       shares border with
# P1464 (11693)     people born here
# P1792 (11669)     assocaited people
# P982 (10690)      musicbrainz id
# P473 (10294)      dialing code
# P2326 (10212)     GNS id
# P1343 (10175)     described by
# P948 (10063)      page banner
# P94 (9821)        coat of arms
# P5573 (9630)      archinform id
# P8168 (9570)      factgrid id
# P1417 (9060)      encyc brit id
# P1448 (8663)      official name
# P9957 (8572)      museum place id
# P3417 (8437)      quora id
# P7867 (8262)      category for maps
# P1465 (8130)      cat for people who died here
# P1705 (7981)      native label
# P1296 (7633)      encyc catalan id
# P12385 (7629)     ditto
# P571 (7610)       inception
# P691 (7419)       nl cr aut id
# P1937 (7337)      un/locode
# P1667 (7214)      TGN
# P7471 (7068)      inat place id
# P190 (7038)       twinned admin body
# P36 (6928)        capital
# P268 (6872)       bnf id
# P1036 (6312)      ddc
# P150 (6225)       contains admin territory
# P395 (6118)       licence plate code
# P4342 (5971)      snl.no id
# P1889 (5901)      diff from
# P41 (5853)        flag image
# P1376 (5793)      capital of
# P935 (5703)       commons gallery
# P7305 (5386)      PWN enc id
# P269 (5040)       idref id
# P8313 (4937)      den store danske id
# P998 (4864)       curlie id
# P6 (4790)         head of govt
# P1313 (4507)      office of head of govt
# P1281 (4276)      woeid
# P206 (4251)
# P1225 (3993)
# P590 (3976)
# P3896 (3945)
# P7197 (3924)
# P2924 (3550)
# P463 (3488)
# P8989 (3361)
# P8119 (3327)
# P1549 (3292)
# P8519 (3269)
# P1538 (3234)
# P1539 (3200)
# P1540 (3185)
# P2163 (3115)
# P138 (3105)
# P361 (3091)
# P10565 (2932)
# P1456 (2914)
# P10622 (2744)
# P3219 (2736)
# P6706 (2707)
# P7818 (2640)
# P968 (2581)
# P2927 (2574)
# P300 (2544)
# P2581 (2473)
# P5063 (2349)
# P31_Q1549591 (2342)
# P8138 (2327)
# P7850 (2314)
# P10397 (2284)
# P11693 (2279)
# P1997 (2278)
# P11012 (2239)
# P8814 (2233)
# P4839 (2218)
# P7938 (2195)
# P901 (2190)
# P1329 (2190)
# P439 (2148)
# P194 (2143)
# P3984 (2131)
# P7959 (2107)
# P4212 (2089)
# P12086 (2063)
# P4672 (2058)
# P8406 (2056)
# P374 (2015)
# P31_Q484170 (2013)
# P8422 (1986)
# P1791 (1970)
# P10832 (1952)
# P5019 (1949)
# P443 (1946)
# P6671 (1918)
# P9037 (1909)
# P610 (1908)
# P213 (1902)
# P1335 (1870)
# P1332 (1867)
# P1333 (1861)
# P1334 (1861)
# P3616 (1843)
# P2936 (1825)
# P774 (1812)
# P442 (1795)
# P2347 (1790)
# P1740 (1786)
# P949 (1777)
# P3120 (1769)
# P8744 (1749)
# P2002 (1740)
# P417 (1739)
# P1365 (1720)
# P31_Q515 (1699)
# P9495 (1698)
# P8408 (1697)
# P950 (1660)
# P2013 (1643)
# P3365 (1642)
# P605 (1641)
# P1830 (1639)
# P1813 (1634)
# P635 (1624)
# P31_Q42744322 (1601)
# P163 (1585)
# P1245 (1566)
# P30 (1560)
# P1617 (1555)
# P2186 (1540)
# P3222 (1514)
# P706 (1487)
# P8714 (1477)
# P6200 (1470)
# P37 (1444)
# P806 (1441)
# P485 (1430)
# P6404 (1421)
# P613 (1420)
# P6832 (1417)
# P9235 (1417)
# P1388 (1414)
# P31_Q747074 (1413)
# P2003 (1400)
# P1936 (1399)
# P4527 (1393)
# P11196 (1385)
# P793 (1350)
# P2043 (1349)
# P3615 (1346)
# P237 (1294)
# P8179 (1285)
# P1151 (1282)
# P8687 (1271)
# P3639 (1271)
# P31_Q3957 (1252)
# P1584 (1218)
# P1943 (1212)
# P349 (1176)
# P6058 (1156)
# P7960 (1120)
# P5008 (1109)
# P1424 (1099)
# P31_Q1093829 (1081)
# P1435 (1071)
# P7982 (1070)
# P8424 (1066)
# P882 (1051)
# P9757 (1046)
# P3134 (1027)
# P836 (1019)
# P2184 (1008)
# P2716 (1001)
