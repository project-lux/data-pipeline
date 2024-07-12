import os
import sys
import csv
import json
import requests

from vertexai.preview.generative_models import GenerativeModel
import vertexai.preview.generative_models as generative_models

### The settings are HIGH / NONE as the answers are not dangerous, but the input might be detected as such
# e.g. a book title that includes the words "child abuse" is not itself child abuse

safety_settings = {
    generative_models.HarmCategory.HARM_CATEGORY_HATE_SPEECH: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
    generative_models.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
    generative_models.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: generative_models.HarmBlockThreshold.BLOCK_NONE,
    generative_models.HarmCategory.HARM_CATEGORY_HARASSMENT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
}

# modelName = "gemini-1.5-pro-001"
modelName = "gemini-1.5-flash-001"  # Flash seems to do well enough

prompt_1 = """You are a very careful geography research assistant who pays very close attention to detail.  
Your goal is to check that the name is really for a place on earth and not for something which is not a place, and that the name is spelled correctly. 
It is okay if the place name is a recognized name for the place, including in languages other than English. It is not okay if there are additional punctuation characters.
Some additional context will be given after the question to help you to decide.
Your response must be in JSON. The response must have a field called "okay" and if the place is spelt correctly, then the value is "Y". 
If the place is not spelled correctly, then the value of the "okay" field is "N" and there is a second field called "name" with the correct spelling for the place. 
If the place is not a place at all, then the value of the "okay" field is "X". 
If you are not confident in your answer then the value of the "okay" field is "?".
For example, a correctly spelled place "Japan" would have the response: {"okay":"Y"}
For example, an incorrectly spelled place name of "Japaan" would have the response: {"okay":"N", "name": "Japan"}
For example, the name "accounting" which is not for a place at all would have the response: {"okay":"X"}

Is "{P1}" a place and spelled correctly?
{FULLBOOK}
{PARENT}
"""

prompt_2 = """You are a very careful geography research assistant who pays very close attention to detail.  
Your goal is to answer a question on whether or not one place is geographically within another place.
Some additional context will be given after the question to help you to decide.
Your response must be in JSON. The response must have a field called "parent" and if the first place is geographically within the second place, then the value is "Y".
If the first place is not geographically within the second place, then the value is "N".
If the first place is not geographically within the second place, but the second place is geographically within the first place, then the value is "reversed".
If you are not confident in your answer, then the value is "?".
For example, if you are asked if Tokyo is geographically within Japan, then the response would be: {"parent": "Y"}
For example, if you are asked if Tokyo is geographically within New Zealand, then response would be: {"parent": "N"}
For example, if you are asked if Japan is geographically within Tokyo, then response would be: {"parent": "reversed"}

Is "{P1}" geographically within a place called "{P2}"?
{FULLBOOK}
{SPELLING}
"""

prompt_3 = """You are a very careful geography research assistant who pays very close attention to detail.  
Your goal is to briefly and factually describe a place in one or two sentences, and to give accurate links to wikipedia for the place.
Some additional context will be given after the question to help you.
Your response must be in JSON. The response has two fields. The first field is called "description" with a value of the brief description.
The second field is called "wp" with a value of the URL of the wikipedia entry for the place. Do not give the URL for a wikimedia disambiguation page or list of places. If you are not confident that the URL is correct, then the value of the "wp" property should be "?" instead.

Please describe and give the URL for the wikipedia page that best describes the place "{P1}" which is geographically within "{P2}".
{FULLBOOK}
{SPELLING}
"""

book_text = """"{P1} might be the subject of a book called "{BOOK}"."""
parent_text = """"{P1}" might be geographically within a place called "{P2}"."""
spelling_text = """"{P1}" is likely also known as "{SPELL}"."""

uri = "https://lux.collections.yale.edu/api/search/work?q=%7B%22AND%22%3A%5B%7B%22aboutConcept%22%3A%7B%22influencedByPlace%22%3A%7B%22identifier%22%3A%22https%3A%2F%2Flinked-art.library.yale.edu%2Fnode%2F{IDENTIFIER}%22%7D%7D%7D%5D%7D"
wmuri = "https://{LANG}.wikipedia.org/w/api.php?format=json&action=query&prop=pageprops&ppprop=wikibase_item&redirects=1&titles={PAGENAME}"


def get_works(identifier):
    resp = requests.get(uri.replace("{IDENTIFIER}", identifier))
    js = resp.json()
    matches = js.get("orderedItems", [])
    if matches:
        wks = []
        while matches and len(wks) < 5:
            wk = matches.pop(0)
            try:
                resp2 = requests.get(wk["id"] + "?profile=name")
                js2 = resp2.json()
                nm = js2["identified_by"][0]["content"]
                wks.append([wk["id"], nm])
            except:
                print(f"Failed to get name of work")
        return wks
    else:
        return []


### Swap this function out for other LLMs
# Takes a prompt text, returns the answer text
def generate(prompt):
    try:
        model = GenerativeModel(modelName)
        responses = model.generate_content(
            [prompt],
            generation_config={"max_output_tokens": 8192, "temperature": 0.2, "top_p": 1, "top_k": 32},
            safety_settings=safety_settings,
            stream=False,
        )
    except:
        # Crashed in google code somewhere
        print("Exception raised by generate")
        return ""
    if not responses.candidates:
        print("No candidates")
    elif not responses.candidates[0].content:
        print("No content")
    elif not responses.candidates[0].content.parts:
        print("No parts")
    elif not responses.candidates[0].content.parts[0].text:
        print("No text")
    else:
        if len(responses.candidates) > 1:
            print("Multiple candidates, picking first:")
            print(responses.candidates)
        return responses.candidates[0].content.parts[0].text.strip()
    print(responses)
    return ""


def make_prompt(p, fields):
    for k, v in fields.items():
        p = p.replace("{" + k + "}", v)
    return p


def to_json(answer):
    if answer.startswith("```json"):
        answer = answer.replace("```json", "")
        answer = answer.replace("```", "")
    # try and load it
    try:
        return json.loads(answer)
    except:
        return None


def write_to_disk(ajs, identifier, child, pid, parent, works):
    ajs["child_id"] = [identifier, child]
    ajs["parent_id"] = [pid.replace("https://linked-art.library.yale.edu/node/", ""), parent]
    ajs["works_id"] = works
    outs = json.dumps(ajs)
    outfh.write(outs)
    outfh.write("\n")
    outfh.flush()
    print(json.dumps(ajs, indent=2))


# This is a simple tab separate value file with four columns:
# identifer -- the identifier for the child place in LUX
# child -- the name of the child place
# pid -- the identifier for the parent place in LUX
# parent -- the name of the parent place
# example line:
#    a8e29dd2-6298-4984-a946-75a1d8f0bb29    Pea River       a08cd7cd-99bb-4fc9-a77f-e81469884021   Alabama

print("Reading in data...")
rows = []
with open("lib_places2.tsv") as fh:
    reader = csv.reader(fh, delimiter="\t")
    for r in reader:
        rows.append(r)

print("Reading in past results...")
results = {}
if os.path.exists("results.jsonl"):
    fh = open("results.jsonl")
    for l in fh.readlines():
        jss = json.loads(l)
        results[jss["child_id"][0]] = jss
    fh.close()
else:
    fh = open("results.jsonl", "w")
    fh.close()

outfh = open("results.jsonl", "a")

print("Starting...")

### TODO: Should test all parents separately with prompt_1 first

for r in rows:
    (identifier, child, pid, parent) = r[:4]
    if identifier in results:
        continue

    print(f" ---- {child} part_of {parent} ----")
    fields = {}

    works = get_works(identifier)

    fields["P1"] = child
    fields["P2"] = parent
    partxt = make_prompt(parent_text, fields)
    if partxt:
        fields["PARENT"] = partxt
    else:
        fields["PARENT"] = ""

    bookts = []
    for book_id, book_text in works:
        fields["BOOK"] = book_text
        bookt = make_prompt(book_text, fields)
        bookts.append(bookt)
    booktss = "\n".join(bookts)
    if booktss:
        fields["FULLBOOK"] = booktss
    else:
        fields["FULLBOOK"] = ""

    prompt = make_prompt(prompt_1, fields)
    sys.stdout.write("1...")
    sys.stdout.flush()
    answer = generate(prompt)
    ajs = to_json(answer)

    if ajs:
        if not "okay" in ajs:
            # very bad response
            print(f"PROMPT 1 FAILED: {ajs}")
            continue
        if ajs["okay"] in ["X", "?"]:
            # Not a place or not confident, write to disk, we're done
            write_to_disk(ajs, identifier, child, pid, parent, works)
            continue

        # We are a place, keep going
        if "name" in ajs:
            fields["SPELL"] = ajs["name"]
            sptxt = make_prompt(spelling_text, fields)
            fields["SPELLING"] = sptxt
        else:
            sptxt = ""

        prompt = make_prompt(prompt_2, fields)
        sys.stdout.write("2...")
        sys.stdout.flush()
        answer = generate(prompt)
        ajs2 = to_json(answer)

        if ajs2:
            if not "parent" in ajs2:
                print(f"PROMPT 2 FAILED: {ajs2}")
                ajs["PROMPT2"] = "BAD JSON"
            else:
                ajs["parent"] = ajs2["parent"]
                if ajs["parent"] in ["?", "N", "reversed"]:
                    # Not confident, not parent, or backwards: don't continue
                    pass
                else:
                    # Proceed to prompt 3 to get more info
                    prompt = make_prompt(prompt_3, fields)
                    sys.stdout.write("3...")
                    sys.stdout.flush()
                    answer = generate(prompt)
                    ajs3 = to_json(answer)
                    if ajs3:
                        if "description" in ajs3:
                            ajs["description"] = ajs3["description"]
                        if "wp" in ajs3 and ajs3["wp"].startswith("http"):
                            ajs["wp"] = ajs3["wp"]
                            try:
                                wpname = ajs["wp"].rsplit("/", 1)[-1]
                                lang = ajs["wp"].replace("https://", "")
                                lang = lang.replace("http://", "")  # just in case
                                lang = lang.split(".")[0]
                                resp = requests.get(wmuri.replace("{PAGENAME}", wpname).replace("{LANG}", lang))
                                js = resp.json()
                                ajs["wp_wd"] = js

                                ###
                                ### TODO
                                ###
                                # Now we should retrieve the JSON for the WD and test
                                # Is it likely a place?
                                # Is it a WM disambiguation page?
                                # If parent is a country, and it has a country field, are they likely the same?
                                # (etc)
                                # If the WD fails, rerun prompt_3 with gemini-1.5-pro

                            except:
                                pass
                    else:
                        print(f"PROMPT 3 FAILED: No reponse")
                        ajs["PROMPT3"] = "NO RESPONSE"
        else:
            print(f"PROMPT 2 FAILED: No response")
            ajs["PROMPT2"] = "NO RESPONSE"
    else:
        print(f"PROMPT 1 FAILED: No Response")
        continue

    write_to_disk(ajs, identifier, child, pid, parent, works)
