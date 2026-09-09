from pipeline.process.base.mapper import Mapper
from cromulent import model, vocab
from shapely.geometry import Polygon
import json
import numpy as np


def _vec2d_dist(p1, p2):
    return (p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2


def _vec2d_sub(p1, p2):
    return (p1[0] - p2[0], p1[1] - p2[1])


def _vec2d_mult(p1, p2):
    return p1[0] * p2[0] + p1[1] * p2[1]


#  ... ... ... 8695907b-1828-4bee-b3b1-fc7abdba43cf
# Fetching https://data.whosonfirst.org/102/051/581/102051581.geojson
# Failed to map 102051581 to Place
# Traceback (most recent call last):
#   File "/Users/rs2668/Development/data/pipeline/./recurse_integration_breadth.py", line 201, in <module>
#     process_rec(ext, False)
#   File "/Users/rs2668/Development/data/pipeline/./recurse_integration_breadth.py", line 49, in process_rec
#     newrec = coller.collect(rec4)
#   File "/Users/rs2668/Development/data/pipeline/pipeline/collector/collect.py", line 159, in collect
#     xrec = mapper.transform(dr, cls)
#   File "/Users/rs2668/Development/data/pipeline/pipeline/sources/wof/mapper.py", line 150, in transform
#     ncoords = ramerdouglas(coords, factor)
#   File "/Users/rs2668/Development/data/pipeline/pipeline/sources/wof/mapper.py", line 19, in ramerdouglas
#     _vec2d_dist(begin, curr) - _vec2d_mult(_vec2d_sub(end, begin), \
# ZeroDivisionError: float division by zero


def ramerdouglas(line, dist):
    """Ramer-Douglas-Peucker simplification.

    Same algorithm and same output points as the original scalar version --
    the per-point squared perpendicular distance is just computed for a whole
    span at once. The coordinates are converted to an array once at the top
    and the recursion carries index ranges, because re-slicing the point list
    (and rebuilding an array) at every level cost more than the vectorisation
    saved. Worth 3-8x depending on how many points each level examines.
    """
    if len(line) < 3:
        return line

    pts = np.asarray(line, dtype=float)
    xs = pts[:, 0]
    ys = pts[:, 1]
    distsq = dist ** 2

    def recurse(lo, hi):
        # inclusive span [lo, hi]; returns indices of the points to keep
        if hi - lo + 1 < 3:
            return list(range(lo, hi + 1))
        bi, ei = (lo, hi) if line[lo] != line[hi] else (lo, hi - 1)
        bx = xs[bi]
        by = ys[bi]
        dx = xs[ei] - bx
        dy = ys[ei] - by
        denom = dx * dx + dy * dy
        if denom == 0:
            raise ValueError("EDIVZERO?")
        px = xs[lo + 1:hi] - bx
        py = ys[lo + 1:hi] - by
        # |curr-begin|^2 - ((end-begin).(curr-begin))^2 / |end-begin|^2
        d2 = (px * px + py * py) - (dx * px + dy * py) ** 2 / denom
        k = int(d2.argmax())
        if d2[k] < distsq:
            return [bi, ei]
        pos = lo + 1 + k
        return recurse(lo, pos) + recurse(pos, hi)[1:]

    return [tuple(pts[i]) for i in recurse(0, len(line) - 1)]


class WofMapper(Mapper):
    def __init__(self, config):
        Mapper.__init__(self, config)
        self.hierarchy_order = ["continent", "country", "macroregion", "region", "county", "locality", "localadmin"]

    def fix_identifier(self, identifier):
        if "/" in identifier or "geojson" in identifier:
            identifier = identifier.split("/")[-1]
            identifier = identifier.replace(".geojson", "")
            return identifier
        return identifier

    def guess_type(self, data):
        return model.Place

    def transform(self, record, rectype, reference=False):
        rec = record["data"]
        props = rec.get("properties", {})

        if not props:
            # Need at least a name!
            return None

        ident = f"{self.namespace}{rec['id']}"

        if rectype != "Place":
            # WoF has great place data, but everything is a place
            # so just return None if asked for a non-Place
            # print(f"Asked WoF for a non-Place {rectype}")
            return None

        top = model.Place(ident=ident)
        for k, v in props.items():
            if v and k.startswith("name:"):
                # name:LLL_x_preferred: [name]
                val = v[0]
                lll = k[5:8]
                ll = self.lang_three_to_two.get(lll, "")
                if ll in self.must_have:
                    nm = vocab.PrimaryName(content=val)
                    nm.language = self.process_langs[ll]
                    top.identified_by = nm
                    if ll == "en":
                        top._label = val
        if not hasattr(top, "_label") and "wof:name" in props:
            top._label = props["wof:name"]
            if not hasattr(top, "identified_by"):
                # Last resort in case all names are filtered by must_have list
                top.identified_by = model.Name(content=props["wof:name"])

        if not hasattr(top, "identified_by"):
            top.identified_by = model.Name(content="Unnamed Place")

        # Equivalent
        if "wof:concordances" in props:
            cc = props["wof:concordances"]
            sames = []
            if "wd:id" in cc:
                sames.append(f"http://www.wikidata.org/entity/{cc['wd:id']}")
            if "gn:id" in cc:
                sames.append(f"https://sws.geonames.org/{cc['gn:id']}")
            if "loc:id" in cc:
                sames.append(f"http://id.loc.gov/authorities/names/{cc['loc:id']}")
            if "tgn:id" in cc:
                sames.append(f"http://vocab.getty.edu/tgn/{cc['tgn:id']}")
            for s in sames:
                top.equivalent = model.Place(ident=s, label=top._label)

        # Parent Place
        # wof:parent_id, and if -1 then look in wof:hierarchy
        parid = props.get("wof:parent_id", -1)
        if parid > 0:
            top.part_of = model.Place(ident=f"{self.namespace}{parid}")
        else:
            pt = props.get("wof:placetype", "")
            if pt in self.hierarchy_order:
                idx = self.hierarchy_order.index(pt)
                if idx:  # strip 0 = continent
                    ppt = self.hierarchy_order[idx - 1]
                    hiers = props.get("wof:hierarchy", [])
                    for h in hiers:
                        ppk = f"{ppt}_id"
                        if ppk in h:
                            top.part_of = model.Place(ident=f"{self.namespace}{h[ppk]}")
                            break

        # Geometry!
        bbox = []
        if "bbox" in rec:
            bbox = rec["bbox"]
        elif "geom:bbox" in props:
            bbox = props["geom:bbox"]
            # String
            if type(bbox) == str:
                bbox = json.loads(f"[{bbox}]")

        if bbox and bbox[0] == bbox[2] and bbox[1] == bbox[3]:
            # it's a point, which we'll get below
            bbox = []

        point = []
        if "lbl:latitude" in props:
            point = [props["lbl:longitude"], props["lbl:latitude"]]
        if "geom:latitude" in props and not point:
            point = [props["geom:longitude"], props["geom:latitude"]]
        if "mps:latitude" in props and not point:
            point = [props["mps:longitude"], props["lbl:latitude"]]

        geom = rec["geometry"]
        t = geom["type"]
        coords = geom["coordinates"]
        if t == "MultiPolygon":
            # Many of these are actually just Polygons
            # No need for geojson/shapely
            while len(coords) == 1:
                coords = coords[0]
            if len(coords[0]) != 2 or type(coords[0][0]) != float:
                # A real multipolygon :( Just use bounding box for now
                coords = []
            else:
                t = "Polygon"
        elif t == "Polygon":
            while len(coords) == 1:
                coords = coords[0]
            if len(coords[0]) != 2 or type(coords[0][0]) != float:
                coords = []
        else:
            coords = []

        if coords:
            if len(coords) > 350:
                factor = 500 / (len(coords) * 10)
                while True:
                    try:
                        ncoords = ramerdouglas(coords, factor)
                    except:
                        coords = []
                        break
                    # print(f"Reduced coordinate space from {len(coords)} to {len(ncoords)}")
                    if len(ncoords) < 100:
                        factor /= 2
                    elif len(ncoords) > 600:
                        factor *= 2
                    else:
                        coords = ncoords
                        break
                coords = [coords]
            else:
                if len(coords) != 1:
                    coords = [coords]

            # somehow can be a point?
            if len(coords) == 1 and len(coords[0]) == 2 and type(coords[0][1]) == float:
                # This will fall out at the area stage anyway
                print(f"point? {coords}")
                print(f"From: {geom['coordinates']}")
                coords = []

            if coords:
                try:
                    rounded = [[round(x[0], 5), round(x[1], 5)] for x in coords[0]]
                except:
                    print(f"FAILED: {coords}")
                    raise
                remove_idx = []
                for x in range(len(coords) - 1):
                    if coords[x] == coords[x + 1]:
                        # drat
                        remove_idx.append(x)
                if remove_idx:
                    remove_idx.reverse()
                    for idx in remove_idx:
                        coords.pop(idx)

                p = Polygon(rounded)
                if p.area * 1000 < 5:
                    # Polygon is so small as to be a point
                    coords = []
                else:
                    coords = [rounded]

        if not coords and bbox:
            # Make a polygon from the bounding box
            # Do rounding here, in case we round it down to a point
            coords = [
                [bbox[0], bbox[1]],
                [bbox[2], bbox[1]],
                [bbox[2], bbox[3]],
                [bbox[0], bbox[3]],
                [bbox[0], bbox[1]],
            ]
            rounded = [[round(x[0], 5), round(x[1], 5)] for x in coords]
            broken = False
            for x in range(len(coords) - 1):
                if coords[x] == coords[x + 1]:
                    # drat
                    broken = True
                    break
            if broken:
                coords = []
            else:
                coords = [rounded]

        if coords:
            p = Polygon(coords[0])
            top.defined_by = p.wkt
        else:
            top.defined_by = f"POINT ({point[0]} {point[1]} )"

        data = model.factory.toJSON(top)
        return {"identifier": record["identifier"], "data": data, "source": "wof"}
