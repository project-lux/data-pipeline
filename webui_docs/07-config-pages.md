# W07. Configuration pages

**Size:** L · **Depends on:** W04, W06
**Read:** `04-config-store-and-schema.md`; `lux_pipeline/config_store.py`,
`config_schema.py` (as merged); `lux_pipeline/cli/list.py` (`source_rows`,
W03); `lux_pipeline/cli/_handler.py` (`refuse_while_building`); W06's
`templates/_macros.html` and `security.py`

## Intent

Browse, edit, validate, save, create, delete and reload configuration
records in the browser, writing exactly what `lux config` writes.

Routes:

| Method, path | What |
|---|---|
| `GET /config` | record list grouped by type; a source table (from `source_rows`) with namespace, order, classes, file |
| `GET /config/{key}` | one record: Fields tab, JSON tab, file path, store, the `lux config show` equivalent |
| `POST /config/{key}/validate` | body: JSON text → problems (JSON response; the page calls it on blur and before save) |
| `POST /config/{key}/diff` | body: JSON text → unified diff |
| `POST /config/{key}` | save: validates, writes, `state.reset()`, redirects with a flash |
| `GET /config/new?type=external` | a form from `config_schema.template` |
| `POST /config/new` | create (`new_record`) |
| `POST /config/{key}/delete` | typed confirmation = the key |
| `POST /config/reload` | `state.reset()` |
| `GET /api/config`, `/api/config/{key}`, `/api/config/schema/{type}` | JSON of the above |

No git integration: the files are written, and committing the configuration
checkout stays in the shell (Rob's decision, 2026-11-02).

## Design

### Record list (`/config`)

`api/config.py: config_index(state) -> {"groups": [{"type", "label", "records": [{"key", "name", "store", "path", "summary"}]}], "sources": source_rows(cfgs, "all"), "building": refuse_while_building(...) would refuse (bool), "config_dir", "stores": [sub-config store keys]}`.
Order of groups: base/system, caches / default, globals, internal,
external, results, map, marklogic, config, build (if any). `summary`:
for a source the namespace; for a map its `storeClass` short name; for
base `base_dir`.

### Record page (`/config/{key}`)

`record_data(state, key) -> {"key", "type", "path", "store", "raw": read_record(...), "text": dumps(raw), "fields": [...], "unknown": [keys not in schema], "defaults": {class key: default class}, "problems": validate_record(...)}`
where `fields` is, for every key in `config_schema.keys_for(type)` that is
present **or** is a `*Class` key with a default, `{"key", "type", "help",
"value", "present", "default", "resolved"}` — `resolved` for a path key is
the absolute path the live cfg has (`cfgs.internal[key][k]` after
`instantiate` resolved it; show "(not resolved until instantiated)" if it
is still relative), for a class key the result of `importObject` ("ok",
"legacy path → new", or the error).

Fields tab: inputs by type (`bool` → checkbox with an "unset" option, `int`
→ number, `list`/`dict` → a small JSON textarea, `class`/`path`/`str` →
text, `enum` → select). Changing a field updates the JSON tab (the JSON is
the source of truth; `editor.js` keeps a single object and re-renders both
tabs from it). Unknown keys are listed under the fields with their raw
value read-only and a "edit in JSON" link.

JSON tab: a `<textarea>` with the record text, a line/column error message
on invalid JSON, *Validate* (POST `/validate`), *Diff* (POST `/diff`,
shown in a `<pre>` with `+`/`-` colouring), *Save*. Save is disabled
while problems exist that are not `warning:`; the save button's label
says "Save (2 warnings)" when only warnings remain. If `building`, the
save button is disabled with a banner, and a checkbox "I know a build is
running; write anyway" enables it and sends `force=1` (the handler passes
`force` to `refuse_while_building`).

After save: flash "Wrote `<path>`; configuration reloaded" and the
record re-rendered from the new file. On a `ConfigError`, re-render with
the problems and the user's text intact (never lose typed text).

### New / delete / reload

New: `?type=` select first (the record types the schema has templates
for), then the same editor on `template(type, name)` with the name field
required and a store select (main directory or a sub-config store).
Delete: a modal (`confirm.js`) asking for the key to be typed; `delete_record`,
then reset. Reload: `state.reset()` then redirect back; the flash says
how many records were read.

## Known hazards

* Never render `cfgs.internal[key]` as the editable JSON: it has objects
  in it. `read_record` only.
* `state.reset()` closes stores; if a Records page request is in flight
  it may fail once. Acceptable; `AppState.lock` serialises the reset.
* `textarea` round trip must preserve the file byte for byte when
  nothing changed: `diff_record` on unchanged text must be empty — tabs
  and key order included. `dumps` is canonical (indent 4), so a file that
  was hand-formatted differently shows a whitespace-only diff on first
  save; the diff view says "formatting only" when `json.loads` of both
  sides is equal.
* CSRF on every POST (W06 middleware); the validate/diff XHRs send the
  header.
* Big records (`base.json`, wikidata) are fine in a textarea; do not add a
  code editor dependency.

## Steps

1. `api/config.py` data functions + JSON routes + tests.
2. List page, record page (JSON tab first), save path with flash and
   error re-render.
3. Fields tab and `editor.js`.
4. New, delete, reload.
5. `docs/web.md` section.

## Tests (`tests/test_web_config.py`)

* `/api/config` lists every record of the fixture with the right type and
  path; sub-config store records show their store.
* `/api/config/aat` `raw` equals the file; `fields` includes `mapperClass`
  with `resolved == "ok"`; a legacy path in a test record shows the
  legacy message.
* `POST /config/aat/validate` with a bad `mapperClass` returns the problem;
  with valid text returns `[]`.
* `POST /config/aat` with `merge_order` changed writes the file (compare
  with `dumps`), reloads (`/api/config/aat` shows the new value, and
  `state.configs().external["aat"]["merge_order"]` too); with invalid JSON
  returns 400 and the response body contains the submitted text.
* While a fake running run exists, save is 409 without `force`, 200 with.
* New record then delete record; `/api/config` reflects both.
* Without `_csrf` every POST is 403.
* The template renders no `[object Object]` and no `&#34;` inside `<script>`
  (the JSON is passed with `tojson`).

## Acceptance criteria

* Editing `merge_order` of a source in the browser produces the same file
  as `lux config set`; `git diff` (on a checkout) shows one changed line.
* Tests pass; `lux config` unchanged.
