// The record page: switch between the Fields and JSON tabs, and check the
// JSON as it is typed (the server says what is wrong, and the save button
// says how many warnings it would save with). Without this script both
// tabs show, one above the other, and the server checks on save.
import { post } from "./app.js";

const pageData = document.getElementById("config-page");
const key = pageData ? JSON.parse(pageData.textContent).key : null;

// --- tabs ----------------------------------------------------------------
const tabs = document.querySelectorAll("[data-tab]");
function show(name) {
  for (const t of tabs) t.setAttribute("aria-selected", String(t.dataset.tab === name));
  for (const p of document.querySelectorAll("[data-panel]")) p.hidden = p.dataset.panel !== name;
  const url = new URL(window.location);
  url.searchParams.set("tab", name);
  url.searchParams.delete("flash");
  history.replaceState(null, "", url);
}
if (tabs.length) {
  const selected = [...tabs].find((t) => t.getAttribute("aria-selected") === "true");
  show(selected ? selected.dataset.tab : "fields");
  for (const t of tabs) t.addEventListener("click", () => show(t.dataset.tab));
}

// --- checking the JSON ------------------------------------------------------
const textarea = document.getElementById("record-text");
const status = document.getElementById("json-status");
const list = document.getElementById("json-problems");
const save = document.getElementById("save-text");
const diffBox = document.getElementById("diff");

function item(text, cls) {
  const li = document.createElement("li");
  li.className = cls;
  li.textContent = text;
  return li;
}

async function check() {
  if (!key || !textarea) return;
  const resp = await post(`/api/config/${encodeURIComponent(key)}/validate`, { text: textarea.value });
  const r = await resp.json();
  list.replaceChildren();
  if (r.json_error) {
    status.textContent = `Not JSON: ${r.json_error}`;
    status.className = "error";
    save.disabled = true;
    save.textContent = "Save the text";
    return;
  }
  for (const p of r.problems) {
    const warning = p.startsWith("warning:");
    list.appendChild(item(warning ? p.slice(9) : p, warning ? "warning" : "error"));
  }
  status.className = "muted";
  status.textContent = r.errors ? `${r.errors} problem(s) to fix before saving` : "Checks out";
  save.disabled = r.errors > 0;
  save.textContent = r.warnings ? `Save the text (${r.warnings} warning${r.warnings > 1 ? "s" : ""})` : "Save the text";
}

async function showDiff() {
  if (!key) return;
  const resp = await post(`/api/config/${encodeURIComponent(key)}/diff`, { text: textarea.value });
  const r = await resp.json();
  diffBox.replaceChildren();
  diffBox.hidden = false;
  if (!r.diff) { diffBox.textContent = "No change"; return; }
  for (const line of r.diff.split("\n")) {
    const span = document.createElement("span");
    span.className = line.startsWith("+") && !line.startsWith("+++") ? "add"
      : line.startsWith("-") && !line.startsWith("---") ? "del" : line.startsWith("@@") ? "hunk" : "";
    span.textContent = line + "\n";
    diffBox.appendChild(span);
  }
}

if (textarea) {
  let timer = null;
  textarea.addEventListener("input", () => {
    clearTimeout(timer);
    timer = setTimeout(check, 500);
  });
  document.getElementById("validate-button")?.addEventListener("click", check);
  document.getElementById("diff-button")?.addEventListener("click", showDiff);
}
