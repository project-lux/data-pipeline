// The run page, live: the worker rows and the log follow the run's files
// over server-sent events, as `lux watch` follows them in a terminal.
// Everything is built with DOM nodes and textContent: log lines are text
// from the workers, never HTML.
import { subscribe } from "./sse.js";

const page = JSON.parse(document.getElementById("run-page").textContent);
const LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"];
const MAX_LINES = 500;           // the console view's panel keeps 500 too
const path = page.rel ? page.rel.split("/").map(encodeURIComponent).join("/") : null;

// --- formatting, as the server formats ------------------------------------------
export function fmtDuration(secs) {
  if (secs === null || secs === undefined) return "-";
  secs = Math.max(0, Math.round(secs));
  const d = Math.floor(secs / 86400), h = Math.floor((secs % 86400) / 3600);
  const m = Math.floor((secs % 3600) / 60), s = secs % 60;
  const two = (n) => String(n).padStart(2, "0");
  if (d) return `${d}d ${two(h)}h`;
  if (h) return `${h}h ${two(m)}m`;
  if (m) return `${m}m ${two(s)}s`;
  return `${s}s`;
}
const fmtN = (n) => (n === null || n === undefined ? "-" : n.toLocaleString("en-US"));

// --- the worker rows ----------------------------------------------------------------
function cell(cls) { const td = document.createElement("td"); td.className = cls; return td; }

function setProgress(td, completed, total) {
  let bar = td.querySelector("progress");
  let num = td.querySelector(".num");
  if (!bar) {
    td.replaceChildren();
    bar = document.createElement("progress");
    bar.className = "bar";
    num = document.createElement("span");
    num.className = "num";
    td.append(bar, " ", num);
  }
  if (total) {
    bar.max = total;
    bar.value = Math.min(completed, total);
    bar.title = `${fmtN(completed)} of ${fmtN(total)}`;
    num.textContent = `${fmtN(completed)}/${fmtN(total)}`;
  } else {
    bar.removeAttribute("value");     // indeterminate: no total yet
    bar.title = `${fmtN(completed)} so far, total not known yet`;
    num.textContent = `${fmtN(completed)}/-`;
  }
}

function setRow(tr, row, label) {
  tr.querySelector(".label").textContent = label;
  setProgress(tr.querySelector(".progress"), row.completed, row.total);
  tr.querySelector(".pct").textContent = row.percent === null ? "-" : `${Math.round(row.percent)}%`;
  tr.querySelector(".elapsed").textContent = fmtDuration(row.elapsed);
  tr.querySelector(".eta").textContent = fmtDuration(row.eta);
}

function workerRow(n) {
  const tbody = document.querySelector("#workers tbody");
  let tr = tbody.querySelector(`tr[data-worker="${n}"]`);
  if (!tr) {
    tr = document.createElement("tr");
    tr.dataset.worker = n;
    const w = cell("w"); w.textContent = `w${n}`;
    tr.append(w, cell("label"), cell("progress"), cell("num pct"), cell("num elapsed"), cell("num eta"));
    const after = [...tbody.rows].find((r) => Number(r.dataset.worker) > n);
    tbody.insertBefore(tr, after || null);
  }
  return tr;
}

// --- a build's phases ---------------------------------------------------------------
function applyPhases(data) {
  const ol = document.getElementById("phases");
  if (!ol || !data.phases) return;
  ol.replaceChildren();
  for (const p of data.phases) {
    const li = document.createElement("li");
    li.className = `state-${p.state.replace(" ", "-")}`;
    li.title = p.commands.join("\n");
    if (p.run && p.run.rel) {
      const a = document.createElement("a");
      a.href = `/runs/${p.run.rel}`;
      a.textContent = p.phase;
      li.appendChild(a);
    } else {
      li.appendChild(document.createTextNode(p.phase));
    }
    const span = document.createElement("span");
    span.textContent = p.state;
    li.append(" ", span);
    ol.appendChild(li);
  }
  const now = document.getElementById("current-step");
  if (!now) return;
  now.replaceChildren();
  const c = data.current;
  if (c) {
    now.append(`Now on ${c.phase}: `);
    const code = document.createElement("code");
    code.textContent = c.argv;
    now.appendChild(code);
    if (c.run && c.run.rel) {
      const a = document.createElement("a");
      a.href = `/runs/${c.run.rel}`;
      a.textContent = "its run";
      now.append(" (", a, ")");
    }
    now.append("; its log follows below.");
  } else if (page.live) {
    now.textContent = "Between steps.";
  }
}

function applyStatus(data) {
  const badge = document.querySelector("#run-state .badge");
  if (badge) { badge.className = `badge state-${data.state}`; badge.textContent = data.state; }
  const took = document.getElementById("run-elapsed");
  if (took) took.textContent = fmtDuration(data.all.elapsed);
  applyPhases(data);
  if (!document.getElementById("workers")) return;
  for (const row of data.rows) {
    if (!row.visible) continue;
    const tr = workerRow(row.n);
    tr.className = row.done ? "done" : row.waiting ? "waiting" : "";
    setRow(tr, row, row.label);
  }
  const foot = document.querySelector('#workers tfoot tr[data-worker="all"]');
  if (foot) setRow(foot, data.all, `${data.all.workers} worker(s)`);
}

// --- the log ----------------------------------------------------------------------------
const log = document.getElementById("log");
const levelSelect = document.getElementById("log-level");
const workerSelect = document.getElementById("log-worker");
const follow = document.getElementById("log-follow");

function shown(el) {
  const floor = LEVELS.indexOf(levelSelect ? levelSelect.value : "DEBUG");
  const worker = workerSelect ? workerSelect.value : "";
  return LEVELS.indexOf(el.dataset.level) >= floor && (worker === "" || el.dataset.worker === worker);
}

function filter() {
  if (!log) return;
  for (const el of log.children) el.hidden = !shown(el);
  if (follow && follow.checked) log.scrollTop = log.scrollHeight;
}

// a worker's name as the page shows it: w3, driver, or "reconcile w3"
// for a line of the step a build is on
function who(l) {
  const w = l.worker === "driver" ? "driver" : `w${l.worker}`;
  return l.phase ? `${l.phase} ${w}` : w;
}

function lineEl(l) {
  const div = document.createElement("div");
  div.className = `line level-${l.level}${l.cont ? " cont" : ""}${l.phase ? " step" : ""}`;
  div.dataset.worker = String(l.worker);
  div.dataset.level = l.level;
  const parts = [["t", l.time || ""], ["lvl", l.cont ? "" : l.level], ["w", who(l)], ["msg", l.text]];
  parts.forEach(([cls, text], i) => {
    const span = document.createElement("span");
    span.className = cls;
    span.textContent = text;
    div.appendChild(span);
    if (i < parts.length - 1) div.appendChild(document.createTextNode(" "));
  });
  div.hidden = !shown(div);
  return div;
}

function appendLines(lines) {
  if (!log) return;
  for (const l of lines) log.appendChild(lineEl(l));
  while (log.children.length > MAX_LINES) log.removeChild(log.firstChild);
  if (follow && follow.checked) log.scrollTop = log.scrollHeight;
}

// a build passing to its next step: a line saying so, then that step's lines
function phaseChange(c) {
  if (!log) return;
  const text = c ? `=== ${c.phase}: ${c.argv} ===` : "=== between steps ===";
  appendLines([{ worker: "driver", time: "", level: "INFO", text, cont: false }]);
}

async function earlier() {
  const worker = workerSelect ? workerSelect.value : "";
  if (worker === "") { alertBox("Choose a worker to read its earlier lines."); return; }
  const before = page.starts[worker];
  if (!before) { alertBox(`w${worker}: that is the start of its log.`); return; }
  const resp = await fetch(`/api/runs/${path}/log?worker=${worker}&before=${before}`,
                           { headers: { Accept: "application/json" } });
  const data = await resp.json();
  page.starts[worker] = data.starts[worker];
  const first = log.firstChild;
  for (const l of data.lines) log.insertBefore(lineEl(l), first);
  if (follow) follow.checked = false;
}

function alertBox(text) {
  let note = document.getElementById("log-note");
  if (!note) {
    note = document.createElement("p");
    note.id = "log-note";
    note.className = "muted";
    log.parentNode.insertBefore(note, log);
  }
  note.textContent = text;
}

function firstError() {
  const el = [...log.children].find((e) => !e.hidden && (e.dataset.level === "ERROR" || e.dataset.level === "CRITICAL"));
  if (!el) { alertBox("No error among the lines shown."); return; }
  if (follow) follow.checked = false;
  el.scrollIntoView({ block: "center" });
  el.classList.add("flash-line");
}

if (log) {
  levelSelect?.addEventListener("change", filter);
  workerSelect?.addEventListener("change", filter);
  follow?.addEventListener("change", filter);
  document.getElementById("log-first-error")?.addEventListener("click", firstError);
  document.getElementById("log-earlier")?.addEventListener("click", earlier);
  filter();
}

// --- live --------------------------------------------------------------------------------
if (page.live && path) {
  const pairs = (o) => Object.entries(o || {}).map(([n, off]) => `${n}:${off}`).join(",");
  let url = `/api/runs/${path}/events?offsets=${encodeURIComponent(pairs(page.offsets))}`;
  if (page.child && page.child.rel) {
    // a build on a step: the step's lines continue from where the page read them
    const childPath = page.child.rel.split("/").map(encodeURIComponent).join("/");
    url += `&child=${childPath}&child_offsets=${encodeURIComponent(pairs(page.child.offsets))}`;
  }
  let first = true;
  subscribe(url, {
    status: applyStatus,
    log: appendLines,
    phase: (c) => {
      // the stream names the step it starts on; the page already shows it
      if (first && page.child && c && c.run && c.run.rel === page.child.rel) { first = false; return; }
      first = false;
      phaseChange(c);
    },
    done: applyStatus,
  });
}
