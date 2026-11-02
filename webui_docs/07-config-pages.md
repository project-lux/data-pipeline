# W07. Configuration pages

**Size:** L · **Depends on:** W04, W06 · **Done:** 2026-11-02
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

`record_data(state, key) -> {"key", "type", "path", "store", "raw": read_record(...), "text": read_text(...), "fields": [...], "unknown": [keys not in schema], "defaults": config_schema.class_defaults(type), "problems": check_record(...)}`
(`text` is the file as it is, never re-dumped: W04 keeps every file's layout)
where `fields` is, for every key in `config_schema.keys_for(type)` that is
present **or** is a `*Class` key with a default, `{"key", "type", "help",
"value", "present", "default", "resolved"}` — `resolved` for a path key is
the absolute path the live cfg has (`cfgs.internal[key][k]` after
`instantiate` resolved it; show "(not resolved until instantiated)" if it
is still relative), for a class key `config_store.resolve_class` ("ok",
"old path → the current one", or the error).

Fields tab: inputs by type (`bool` → checkbox with an "unset" option, `int`
→ number, `list`/`dict` → a small JSON textarea, `class`/`path`/`str` →
text, `enum` → select). Changing a field updates the JSON tab (the JSON is
the source of truth; `editor.js` keeps a single object and re-renders both
tabs from it). Unknown keys are listed under the fields with their raw
value read-only and a "edit in JSON" link.

Saving: the Fields tab saves with `write_record` (only the changed keys
change, in the file's layout); the JSON tab saves the textarea with
`write_text` (verbatim: the person's layout). Both return `(path,
warnings)`.

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
* The textarea holds the file's text (`read_text`), so an unchanged save
  is byte for byte the file; for the Fields tab, `diff_record` of
  unchanged data is empty and `write_record` does not touch the file
  (W04).
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

## As built

`web/api/config.py` (data functions and JSON routes), `web/routers/config.py`
(pages), `templates/config/{index,record,new}.html`, `static/editor.js`.
Differences from the design above:

* **The two tabs are two forms, not one synced object.** Re-rendering the
  JSON tab from an object would re-dump the text and lose the file's
  layout, which W04 keeps. The Fields form posts `field:<key>` values
  (`mode=fields`) and is saved with `write_record`, so only changed keys
  change; the JSON form posts its text (`mode=text`) and is saved
  verbatim with `write_text`. Both work without JavaScript; `editor.js`
  switches tabs and checks the JSON as it is typed (`/api/config/<key>/validate`),
  shows the diff, and labels the save button with the warning count.
* **Fields:** a select for a boolean ("not set", true, false), a number for
  an integer, a JSON textarea for a list or dict, text otherwise; in a
  class field `null` switches the component off. An empty text field
  keeps an empty string on a key that has one (the caches' `host`), and a
  *remove* box takes the key out. Keys the catalogue does not know are
  listed read-only and changed on the JSON tab. A class key with a default
  shows the default when absent; a path shows where it resolves and
  whether it exists; a class its state (ok, off, old path, error).
* **The routes** for the actions are `/config/_new` and `/config/_reload`:
  a key cannot start with `_`, so a record called `new` or `reload` is
  never hidden. Checking and diffing are JSON routes,
  `POST /api/config/<key>/validate|diff` with `{"text"}` or `{"data"}`.
* **Deleting** asks for the key to be typed in the form (checked by the
  server) rather than in a script dialog.
* **Messages** after a redirect are kept in the server (`app.flash`,
  `redirect_with`: `?flash=<id>`), never taken from the URL, so a link
  cannot make a page say something; `redirect_with` only goes to a path on
  this server (`_reload`'s `back` cannot send you elsewhere).
* **Refusals keep what was typed:** a save refused while a build runs (409,
  with the "write anyway" box), a field or text that is not JSON (400),
  or a record that does not validate (400) re-renders the page with the
  submitted values and the problems.
* An unknown record is 404.

Checked against the live configurations: every record of the build
machine's copies (37) and of pipeline-configs (44) renders as a page and as
JSON. On a scratch copy of pipeline-configs, saving `merge_order` through
a running server's Fields form, with all 25 of its fields submitted,
wrote a file identical to `lux config set aat merge_order 11` on another
copy, one changed line in the whole tree.

## Tests (`tests/test_web_config.py`)

* `/api/config` lists every record of the fixture with the right type and
  path; sub-config store records show their store.
* `/api/config/aat` `raw` equals the file; `fields` includes `mapperClass`
  with `resolved == "ok"`; a legacy path in a test record shows the
  legacy message.
* `POST /config/aat/validate` with a bad `mapperClass` returns the problem;
  with valid text returns `[]`.
* `POST /config/aat` with `merge_order` changed writes the file (one
  changed line), reloads (`/api/config/aat` shows the new value, and
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
