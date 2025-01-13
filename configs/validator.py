import json
import os
from jsonschema import validate, ValidationError, SchemaError


# Load the JSON schema
with open('schema.json', 'r') as schema_file:
    schema = json.load(schema_file)

# Load the JSON data to be validated

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
