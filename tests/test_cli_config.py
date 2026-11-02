"""Web UI item W04: `lux config`, run through the entry point on the W00
installation with a sub-config store. Nothing connects to a database:
the configuration is read, never instantiated."""

import json
import logging
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline import runtime
from lux_pipeline.cli import entry
from lux_pipeline.cli.introspect import NOT_RUNNABLE, command_spec
from webui_fixtures import add_subconfig_store, make_installation, use_installation, write_fake_run

LOGGER = logging.getLogger("lux_pipeline")


@pytest.fixture(autouse=True)
def _restore_logger():
    saved = (list(LOGGER.handlers), LOGGER.level)
    yield
    for h in list(LOGGER.handlers):
        LOGGER.removeHandler(h)
    for h in saved[0]:
        LOGGER.addHandler(h)
    LOGGER.setLevel(saved[1])


@pytest.fixture
def root(tmp_path, monkeypatch):
    root = make_installation(tmp_path / "inst")
    add_subconfig_store(root, "sources", move=("aat",))
    use_installation(monkeypatch, root)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.delenv("VISUAL", raising=False)
    return root


def lux(capsys, *argv):
    runtime.reset_configs()
    code = entry.run(["config", *argv])
    out = capsys.readouterr()
    return code, out.out, out.err


def aat_file(root):
    return root / "subs" / "sources_cache" / "aat.json"


def test_list_and_show(root, capsys):
    code, out, _ = lux(capsys, "list")
    assert code == 0 and out.splitlines()[0].split() == ["KEY", "TYPE", "NAME", "STORE", "FILE"]
    aat = next(l for l in out.splitlines() if l.startswith("aat "))
    assert aat.split()[:4] == ["aat", "external", "aat", "sources"] and str(aat_file(root)) in aat
    code, out, _ = lux(capsys, "show", "aat")
    assert code == 0 and out == aat_file(root).read_text()
    code, out, _ = lux(capsys, "show", "aat", "--path")
    assert out.strip() == str(aat_file(root))
    code, _, err = lux(capsys, "show", "aatt")
    assert code == 1 and "no configuration record 'aatt'" in err


def test_check(root, capsys):
    code, out, _ = lux(capsys, "check")
    assert code == 0 and out.strip().endswith("record(s) checked: 0 error(s), 0 warning(s)")
    text = aat_file(root).read_text().replace('"merge_order": 10', '"merge_order": "ten"')
    aat_file(root).write_text(text)
    code, out, err = lux(capsys, "check", "aat", "ycba")
    assert code == 1
    assert "aat: merge_order should be int, not str ('ten')" in out
    assert "2 record(s) checked: 1 error(s)" in out and "1 error(s) in the configuration" in err


def test_set_and_unset_write_only_what_changed(root, capsys):
    before = aat_file(root).read_text()
    code, out, _ = lux(capsys, "set", "aat", "merge_order", "11")
    assert code == 0 and '-  "merge_order": 10,' in out and '+  "merge_order": 11,' in out
    assert f"wrote {aat_file(root)}" in out
    assert aat_file(root).read_text() == before.replace('"merge_order": 10', '"merge_order": 11')
    code, out, _ = lux(capsys, "set", "aat", "merge_order", "11")
    assert out.strip() == "no change"
    code, out, _ = lux(capsys, "set", "aat", "indexes.reconcile_label.path", "label_to_aat.lmdb")
    assert code == 0
    assert json.loads(aat_file(root).read_text())["indexes"] == {"reconcile_label": {"path": "label_to_aat.lmdb"}}
    code, out, _ = lux(capsys, "unset", "aat", "indexes")
    assert code == 0 and json.loads(aat_file(root).read_text()) == json.loads(
        before.replace('"merge_order": 10', '"merge_order": 11'))


def test_a_bad_value_is_refused_and_nothing_written(root, capsys):
    base = root / "config_cache" / "base.json"
    before = base.read_text()
    code, _, err = lux(capsys, "set", "base", "max_workers", "many")
    assert code == 1 and "max_workers should be int, not str ('many')" in err
    assert base.read_text() == before
    code, _, err = lux(capsys, "set", "aat", "merge_order")
    assert code == 1 and "takes KEY PATH VALUE" in err


def editor(tmp_path, monkeypatch, body):
    """$EDITOR: a python script given the file to edit."""
    script = tmp_path / "editor.py"
    script.write_text("import sys\npath = sys.argv[1]\ntext = open(path).read()\n" + body)
    monkeypatch.setenv("EDITOR", f"{sys.executable} {script}")


def test_edit_saves_the_editors_text_verbatim(root, tmp_path, monkeypatch, capsys):
    editor(tmp_path, monkeypatch, "open(path, 'w').write(text.replace('\"merge_order\": 10', '\"merge_order\":12'))\n")
    before = aat_file(root).read_text()
    code, out, _ = lux(capsys, "edit", "aat")
    assert code == 0 and "wrote" in out
    assert aat_file(root).read_text() == before.replace('"merge_order": 10', '"merge_order":12')


def test_edit_without_a_change_and_with_a_broken_one(root, tmp_path, monkeypatch, capsys):
    editor(tmp_path, monkeypatch, "pass\n")
    code, out, _ = lux(capsys, "edit", "aat")
    assert (code, out.strip()) == (0, "no change")
    before = aat_file(root).read_text()
    editor(tmp_path, monkeypatch, "open(path, 'w').write('{broken')\n")
    code, _, err = lux(capsys, "edit", "aat")
    assert code == 1 and "not JSON" in err and "Your edit is kept in" in err
    kept = err.split("Your edit is kept in ")[1].strip()
    assert Path(kept).read_text() == "{broken"
    os.remove(kept)
    assert aat_file(root).read_text() == before


def test_diff_from_a_file(root, tmp_path, capsys):
    data = json.loads(aat_file(root).read_text())
    data["merge_order"] = 3
    new = tmp_path / "aat-new.json"
    new.write_text(json.dumps(data))
    code, out, _ = lux(capsys, "diff", "aat", "--file", str(new))
    assert code == 0 and '+  "merge_order": 3,' in out
    assert json.loads(aat_file(root).read_text())["merge_order"] == 10    # diff writes nothing


def test_new_prints_a_template_or_writes_a_record(root, tmp_path, capsys):
    code, out, err = lux(capsys, "new", "ulan", "--type", "external")
    assert code == 0 and json.loads(out)["name"] == "ulan" and "Fill in: namespace is empty" in err
    assert not (root / "config_cache" / "ulan.json").exists()
    rec = tmp_path / "ulan.json"
    rec.write_text(json.dumps({"name": "ulan", "type": "external",
                               "namespace": "http://vocab.getty.edu/ulan/",
                               "matches": ["vocab.getty.edu/ulan/"]}))
    code, out, _ = lux(capsys, "new", "ulan", "--store", "sources", "--file", str(rec))
    path = root / "subs" / "sources_cache" / "ulan.json"
    assert code == 0 and f"wrote {path}" in out and path.exists()
    code, _, err = lux(capsys, "new", "ulan", "--file", str(rec))
    assert code == 1 and "already exists in store sources" in err


def test_delete_asks_unless_forced(root, capsys):
    code, _, err = lux(capsys, "delete", "aat")
    assert code == 1 and "without --force" in err and aat_file(root).exists()
    code, out, _ = lux(capsys, "delete", "aat", "--force")
    assert code == 0 and not aat_file(root).exists()


def test_writes_are_refused_while_a_build_runs(root, capsys):
    write_fake_run(root / "data" / "logs", "reconcile", 1)
    code, _, err = lux(capsys, "set", "aat", "merge_order", "12")
    assert code == 1 and "a build is running" in err
    code, out, _ = lux(capsys, "set", "aat", "merge_order", "12", "--force")
    assert code == 0 and "wrote" in out
    # reading is never refused
    assert lux(capsys, "check", "aat")[0] == 0


def test_not_run_from_a_form():
    assert "config" in NOT_RUNNABLE and command_spec("config")["runnable"] is False
