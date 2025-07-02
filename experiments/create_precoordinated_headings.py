import os
import json
from dotenv import load_dotenv
from pipeline.config import Config

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
cfgs.cache_globals()
cfgs.instantiate_all()

output = {}
data_dir = cfgs.data_dir

yul = cfgs.internal['ils']['datacache']

for recs in yul.iter_records_type("Type"):
    data = recs.get("data", {})
    uri = data.get("id","")
    if "created_by" in data:
        if "influenced_by" in data["created_by"]:
            influenced_by = data["created_by"]["influenced_by"]
            output[uri] = influenced_by

# Save the output to a JSON file
output_file = os.path.join(data_dir, "precoordinated_headings.json")
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(output, f)