import json
import os
from jsonschema import validate, ValidationError, SchemaError

"""
This script validates JSON files in the current directory against a JSON schema.

The script performs the following steps:
1. Loads a JSON schema from a file named `schema.json`.
2. Iterates through all JSON files in the current directory, excluding `schema.json` and `base_download.json`.
3. Validates each JSON file against the schema using the `jsonschema` library.
4. Prints whether each file is valid or provides details of any errors encountered.

"""


with open('schema.json', 'r') as schema_file:
    schema = json.load(schema_file)

current_dir = os.path.dirname(os.path.abspath(__file__))
for filename in os.listdir(current_dir):
    if filename.endswith(".json") and filename not in ["schema.json","base_download.json"]:
        json_path = os.path.join(current_dir, filename)
        print(f"Validating file: {filename}")
        try:
            with open(json_path, 'r') as data_file:
                data = json.load(data_file)
                validate(instance=data, schema=schema)
                print("JSON data is valid.")
        except ValidationError as e:
            print(f"JSON data is invalid. Error: {e.message}")
        except SchemaError as e:
            print(f"Schema is invalid. Error: {e.message}")
        except json.JSONDecodeError as e:
            print(f"{filename}: Failed to parse JSON. Error: {e}")
        except Exception as e:
            print(f"{filename}: Unexpected error. Error: {e}")
