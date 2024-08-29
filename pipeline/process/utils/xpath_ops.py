# given JSON, generate XML with the same structure
# don't need to deal with arbitrary JSON, only linked-art JSON-LD
# Should result in the obvious xpaths being valid, rather than injecting elements
# and then use the XML to find the XPath given, and find it in the JSON
# and then manipulate the JSON (yes, the XML is entirely throwaway)

from lxml import etree


def escape_xml(s):
    s = s.replace("&", "&amp;")
    s = s.replace('"', "&quot;")
    s = s.replace("'", "&apos;")
    s = s.replace("<", "&lt;")
    s = s.replace(">", "&gt;")
    return s


def fix(key):
    return key.replace("@", "__")


def convert(what, output):
    if type(what) == dict:
        for k, v in what.items():
            tag = fix(k)
            if type(v) != list:
                v = [v]
            for item in v:
                output.append(f"<{tag}>")
                convert(item, output)
                output.append(f"</{tag}>")
    elif type(what) == str:
        output.append(escape_xml(what))
    elif type(what) in [int, float]:
        output.append(str(what))
    else:
        print(f"Unknown (to model) type: {type(what)} from {what}")


def dicttoxml(what):
    output = ["<record>"]
    convert(what, output)
    output.append("</record>")
    xml = "".join(output)
    return xml


def xpath_on_record(what, xpath):
    xml = dicttoxml(what)
    try:
        dom = etree.XML(xml)
    except:
        print(f"failed to parse XML:\n{xml}")
        return []
    tree = dom.getroottree()
    try:
        matches = dom.xpath("/record" + xpath)
    except:
        print(f"Failed to compile xpath: {xpath}")
        return []
    paths = []
    for m in matches:
        paths.append(tree.getpath(m).replace("/record", ""))
    return paths


def process_operation(what, xpath, operation, argument=None):
    paths = xpath_on_record(what, xpath)
    paths.reverse()  # process from end to beginning to avoid indexes changing
    for p in paths:
        bits = p[1:].split("/")
        path = []
        for bit in bits:
            if bit[-1] == "]":
                sqidx = bit.find("[")
                # indexes are 1 based, not 0 based
                idx = int(bit[sqidx + 1 : -1]) - 1
                key = bit[:sqidx]
            else:
                key = bit
                idx = 0
            path.append((key, idx))

        tgt = what
        for p in path[:-1]:
            if p[0] in tgt:
                tgt = tgt[p[0]]
            if type(tgt) == list:
                tgt = tgt[p[1]]

        if operation == "DELETE":
            val = tgt[path[-1][0]]
            if type(val) == list:
                del tgt[path[-1][0]][path[-1][1]]
                # And if now empty, delete null value key
                if tgt[path[-1][0]] == []:
                    del tgt[path[-1][0]]
            else:
                del tgt[path[-1][0]]
        elif operation == "UPDATE":
            tgt[path[-1][0]] = argument
        else:
            print(f"Unknown operation: {operation}")

    return what


if __name__ == "__main__":
    import requests

    resp = requests.get("https://lux.collections.yale.edu/data/object/436afada-ac05-455e-922a-2a99dfb040b2")
    js = resp.json()
    del js["_links"]
