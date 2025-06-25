import re
import time
import requests
from dateutil import parser
from dateparser.date import DateDataParser
from edtf import parse_edtf, text_to_edtf, struct_time_to_datetime
from datetime import datetime, timedelta
from edtf.parser.parser_classes import UncertainOrApproximate as UOA, PartialUncertainOrApproximate as PUOA
import numpy as np

# Note -- MaskedPrecision was removed from edtf, so removing as fuzzy parser

try:
    from pyluach.dates import HebrewDate
except:
    HebrewDate = None

# ABANDON ALL HOPE, YE WHO ENTER HERE
# Lasciate ogne speranza, voi ch'intrate

default_dt = datetime.strptime("0001-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S")
dp_settings = {"PREFER_DAY_OF_MONTH": "first", "PREFER_DATES_FROM": "past"}
dp_parser = DateDataParser(settings=dp_settings)
# x_re = re.compile('([?xXu0-9-])([?X])')
bcdate_re = re.compile("(.+) ([bceBCE.]+)")
np_precisions = ["", "Y", "M", "D", "h", "m", "s"]  # number of -s in the string
max_life_delta = np.datetime64("2122-01-01") - np.datetime64("2000-01-01")
non_four_year_date = re.compile("(-?)([0-9]{2,3})(-[0-9][0-9]-[0-9][0-9]([^0-9].*|$))")
de_bc_abbr = re.compile("(([0-9][0-9]).([0-9][0-9]).)?v([0-9]{2,3})$")
valid_date_re = re.compile(r"([0-2][0-9]{3})(-[0-1][0-9]-[0-3][0-9]([ T][0-2][0-9]:[0-5][0-9]:[0-5][0-9]Z?$|$))")

time_rectype = {
    "Person": ["born", "died", "carried_out", "participated_in"],
    "Group": ["formed_by", "dissoved_by", "carried_out", "participated_in"],
    "HumanMadeObject": ["produced_by", "encountered_by", "destroyed_by"],
    "DigitalObject": ["created_by", "used_for"],
    "Set": ["created_by", "used_for", "members_exemplified_by"],
    "LinguisticObject": ["created_by", "used_for"],
    "VisualItem": ["created_by", "used_for"],
    "Event": ["timespan"],
    "Activity": ["timespan"],
    "Period": ["timespan"],
    "Type": [],
    "Material": [],
    "Language": [],
    "Currency": [],
    "Place": [],
    "MeasurementUnit": [],
}
timestamp_props = ["begin_of_the_begin", "end_of_the_begin", "begin_of_the_end", "end_of_the_end"]


def get_wikidata_qid(wikipedia_url):
    """
    Given a Wikipedia URL, this function uses Wikidata sitelinks to retrieve the corresponding Wikidata QID.

    Args:
        wikipedia_url (str): The full URL of a Wikipedia page (e.g., "http://en.wikipedia.org/wiki/Addison_Mizner")

    Returns:
        str or None: The Wikidata QID (e.g., "Q466693") if found, otherwise None.
    """
    try:
        title = wikipedia_url.rstrip("/").split("/")[-1]

        endpoint = "https://www.wikidata.org/w/api.php"
        params = {"action": "wbgetentities", "sites": "enwiki", "titles": title, "format": "json"}

        response = requests.get(endpoint, params=params)
        data = response.json()

        entities = data.get("entities", {})
        for qid, entity in entities.items():
            if qid != "-1":  # if page is not found, the key will be "-1"
                return qid
    except Exception as e:
        print(f"Error getting wikidata ID {e}")

    return None


def walk_for_timespan(nodes):
    if type(nodes) != list:
        nodes = [nodes]
    for node in nodes:
        if "timespan" in node:
            # found ya
            ts = node["timespan"]
            for tsp in timestamp_props:
                if tsp in ts:
                    dt = ts[tsp]
                    if dt.endswith("Z"):
                        dt = dt[:-1]
                    try:
                        (b, e) = make_datetime(dt)
                    except:
                        if not dt.startswith("9999"):
                            print(f"Failed to make a date from {dt} ; stripping")
                        del ts[tsp]
                        continue
                    if tsp.startswith("begin"):
                        if dt != b:
                            # print(f"make datetime suggests: {b} for {dt}")
                            ts[tsp] = b
                    elif tsp.startswith("end"):
                        if dt != e:
                            # print(f"make datetime suggests: {e} for {dt}")
                            ts[tsp] = e
        if "part" in node:
            # descend
            walk_for_timespan(node["part"])
        if node["type"] in time_rectype:
            # continue to walk
            for p in time_rectype[node["type"]]:
                if p in node:
                    walk_for_timespan(node[p])


def validate_timespans(rec):
    if not rec["type"] in time_rectype:
        print(f"Couldn't find timespan paths for {rec['type']} in mapper_utils")
    else:
        props = time_rectype[rec["type"]]
        for p in props:
            if p in rec:
                walk_for_timespan(rec[p])


def get_year_from_timespan(event):
    try:
        ts = event["timespan"]["begin_of_the_begin"]
        if ts.startswith("-"):
            dt = ts.split("T")[0].split("-")[1]
            if startswith("0"):
                dt = "-" + dt[1:]
            else:
                dt = "-" + dt
        else:
            dt = ts.split("T")[0].split("-")[0]
    except:
        dt = None
    return dt


def test_birth_death(person):
    if type(person) == dict:
        # JSON serialization
        if "born" in person and "timespan" in person["born"]:
            bts = person["born"]["timespan"]
            if "begin_of_the_begin" in bts:
                b = bts["begin_of_the_begin"]
            else:
                return True
        else:
            return True
        if "died" in person and "timespan" in person["died"]:
            dts = person["died"]["timespan"]
            if "end_of_the_end" in dts:
                d = dts["end_of_the_end"]
            else:
                return True
        else:
            return True
        # if delta birth, death > 122 years, then fail
        # use np to deal with BC dates
    else:
        # it's a crom instance being built by a mapper
        if (
            hasattr(person, "born")
            and hasattr(person.born, "timespan")
            and hasattr(person.born.timespan, "begin_of_the_begin")
        ):
            b = person.born.timespan.begin_of_the_begin
        else:
            return True
        if (
            hasattr(person, "died")
            and hasattr(person.died, "timespan")
            and hasattr(person.died.timespan, "end_of_the_end")
        ):
            d = person.died.timespan.end_of_the_end
        else:
            return True

    try:
        start = np.datetime64(b)
        end = np.datetime64(d)
    except:
        return True
    if end - start > max_life_delta:
        # Can't live longer than 122 years
        return False
    elif end < start:
        # Can't die before being born
        return False
    else:
        return True


def convert_hebrew_date(dt):
    if HebrewDate is not None and int(dt[:4]) > 4500:
        # most likely hebrew calendar ; 4500 = 740 CE
        if "T" in dt:
            ymd = dt.split("T")[0]
        else:
            ymd = dt
        y, m, d = ymd.split("-", 3)
        h = HebrewDate(int(y), int(m), int(d))
        g = h.to_greg()
        dt = g.to_pydate().isoformat() + "T00:00:00"
    return dt


def process_np_datetime(dt, value):
    is_bc = dt.astype("<M8[s]").astype(np.int64) < -62135596800
    start = dt.astype("<M8[s]").astype(str)
    if ":" in value or "T" in value:
        mod = value.count(":") + 1
    else:
        mod = 0
    if value in ["Y", "M", "D", "h", "m", "s"]:
        prec = value
    elif is_bc:
        prec = np_precisions[value.count("-") + mod]
    else:
        prec = np_precisions[value.count("-") + mod + 1]

    delta = np.timedelta64(1, prec)
    enddt = dt + delta
    enddt = enddt - np.timedelta64(1, "s")
    end = str(enddt.astype("<M8[s]").astype(str))
    # ensure we have 4+ digit year
    if is_bc:
        if start[1:].find("-") == 3:
            start = "-0" + start[1:]
        if end[1:].find("-") == 3:
            end = "-0" + end[1:]
    return (start, end)


def make_datetime(value, precision=""):
    # given a date / datetime string
    # and maybe a precision from wikidata
    # return (begin, end) range

    initialValue = value
    value = value.strip()
    # allow 0000-01-01
    if not value or value.startswith("9999") or value == "0000" or "jh" in value.lower():
        return None
    if len(value) > 34:
        # VERY unlikely to be a parsable date
        # max: "19th September, 2002 AD 10:00:00"
        return None

    value = value.replace("edtf", "")
    if value[0] == "[" and value[-1] == "]":
        value = value[1:-1]

    is_bce_date = False
    value = value.strip()
    if value.startswith("- "):
        value = "-" + value[1:].strip()

    # 12-01-01T00:00:00 should be 0012-01-01 not 2012-01-01 or 2001-12-01
    # but 20-07-1976 shouldn't be 0020-07-1976

    if value.startswith("0000-12-31") or value.startswith("0000-01-01"):
        value = f"0001{value[4:]}"

    m = non_four_year_date.match(value)
    if m:
        (bc, year, rest, ignore) = m.groups()
        yy = year.rjust(4, "0")
        value = f"{bc}{yy}{rest}"

    # 1580-09-00T00:00:00
    if "-00T00:00:00" in value:
        value = value.split("T")[0]

    # 13.07.v100 ; 20.07.v356  (v = BC) --> -0100-07-13
    m = de_bc_abbr.match(value)
    if m:
        ignore, dd, mm, yy = m.groups()
        if dd and mm:
            value = f"-{yy}-{mm}-{dd}"
        else:
            value = f"-{yy.rjust(4, '0')}"

    # 197208 --> 1972-08
    if len(value) == 6 and value.isnumeric():
        value = f"{value[:4]}-{value:4:}"

    if value[0] == "-" and value[1].isnumeric():
        # -1
        try:
            dt = np.datetime64(value)
            # We can return straight away!
        except:
            # not well formed ISO
            # Could still be EDTF
            is_bce_date = True
            value = value[1:]
            dt = None
        if dt is not None:
            if not precision:
                return process_np_datetime(dt, value)
            else:
                return process_np_datetime(dt, precision)
    elif "bc" in value.lower() or "b.c" in value.lower():
        # 1000 BC or 1000 BCE
        m = bcdate_re.match(value)
        if m:
            is_bce_date = True
            value = m.group(1)
            # likely just a year, so try and reparse with -

            try:
                dt = np.datetime64("-" + value)
            except:
                # worth a try
                dt = None
            if dt is not None:
                if not precision:
                    return process_np_datetime(dt, "-" + value)
                else:
                    return process_np_datetime(dt, precision)
        # else: implausible but it might be a valid date in another locale
        # so just pass it through
    elif valid_date_re.match(value):
        try:
            dt = np.datetime64(value)
        except:
            # I guess not
            dt = None
        if dt is not None:
            if not precision:
                return process_np_datetime(dt, value)
            else:
                return process_np_datetime(dt, precision)

    # dateutils treats year with leading 0s as current century :(
    # e.g.parser.parse("0052") --> datetime.datetime(2052, 12, 18, 0, 0)
    if len(value) == 4 and value.startswith("00"):
        value = f"{value}-01-01"
        # force the precision to year
        precision = "Y"

    # First try dateutil's parser
    end = None
    try:
        begin = parser.parse(value, default=default_dt)
    except:
        # Nope, try the edtf parser
        # Fix: 2020?
        if len(value) == 5 and value[4] == "?":
            value = value[:4] + "~"
        # Fix: 19XX or 17??
        if "?" in value or "u" in value or "x" in value:
            # value = x_re.sub('\g<1>u', value)
            # value = x_re.sub('\g<1>u', value)
            value = value.replace("u", "X")
            value = value.replace("x", "X")
            value = value.replace("?", "X")
            value = value.replace(".XX.XX", "-XX-XX")
            if value.startswith("XX.XX.") or value.startswith("XX-XX-") or value.startswith("XX XX "):
                value = value[6:]

        value = value.replace("-00", "-XX")
        ed_value = "-" + value if is_bce_date else value

        try:
            # Could be actual EDTF but not parsable by dateutils
            dt = parse_edtf(ed_value)
        except:
            # Not straight edtf, but there's a converter...
            ed_value = text_to_edtf(ed_value)
            if ed_value:
                try:
                    dt = parse_edtf(ed_value)
                except:
                    # Couldn't parse with EDTF
                    dt = None
            else:
                dt = None

        if dt is not None:
            # Yes to EDTF
            if type(dt) in [UOA, PUOA]:
                begin = dt.lower_fuzzy()
                end = dt.upper_fuzzy()
            else:
                try:
                    begin = dt.lower_strict()
                    end = dt.upper_strict()
                except Exception as e:
                    print(f"EDTF couldn't process {ed_value}; ignoring")
                    return None
            if end.tm_year == 9999 or begin.tm_year == 0:
                # garbage
                return None
            try:
                start = struct_time_to_datetime(begin).isoformat()
                end = struct_time_to_datetime(end).isoformat()
            except:
                # BCE dates
                start = time.strftime("%Y-%m-%dT%H:%M:%SZ", begin)
                end = time.strftime("%Y-%m-%dT%H:%M:%SZ", end)
            if is_bce_date and start[1:].find("-") == 3:
                start = "-0" + start[1:]
            # need to -1 sec from end
            try:
                enddt = np.datetime64(end)
                enddt = enddt - np.timedelta64(1, "s")
                end = enddt.astype("<M8[s]").astype(str)
            except:
                # out of range date, e.g. feb 29 on a non leap year
                return None
            if is_bce_date and end[1:].find("-") == 3:
                end = "-0" + end[1:]
            else:
                # might be hebrew
                start = convert_hebrew_date(start)
                end = convert_hebrew_date(end)
            return (start, end)
        else:
            # No to EDTF, last resort try DateDataParser
            try:
                dt3 = dp_parser.get_date_data(value)
                if dt3.period == "day" and dt3.locale != "en":
                    begin = dt3.date_obj
                    end = begin + timedelta(days=1)
                elif dt2:
                    print(f"dateparser found: {dt3} from {value} ?")
                    return None
                else:
                    print(f"Failed to parse date: {initialValue} --> {value}")
                    return None
            except:
                print(f"Failed to parse date: {initialValue} --> {value}")
                return None

    if not precision:
        # Now we will have begin
        prec_dt = dp_parser.get_date_data(value)
        if prec_dt.date_obj:
            prec = prec_dt.period[0].upper()
            if prec == "D":
                # check for HH:MM:SS
                do = prec_dt.date_obj
                if do.second > 0:
                    prec = "s"
                elif do.minute > 0:
                    prec = "m"
                elif do.hour > 0:
                    prec = "h"
        else:
            if begin.second > 0:
                prec = "s"
            elif begin.minute > 0:
                prec = "m"
            elif begin.hour > 0:
                prec = "h"
            elif begin.day != 1:
                prec = "D"
            elif begin.month != 1:
                prec = "M"
            else:
                prec = "Y"
    else:
        prec = precision

    l = {"D": 10, "M": 7, "Y": 4, "h": 13, "m": 16, "s": 19}[prec]

    if is_bce_date:
        dt = np.datetime64("-" + begin.isoformat()[:l])
        return process_np_datetime(dt, prec)
    else:
        if begin.year > 4500:
            dtstr = convert_hebrew_date(begin.isoformat())
        else:
            dtstr = begin.isoformat()[:l]
        dt = np.datetime64(dtstr)
        return process_np_datetime(dt, prec)
