// A JSON value as a collapsible tree, built from DOM nodes and textContent
// only (records hold arbitrary strings: never innerHTML). Objects and
// arrays are <details>; the first levels open. A long array shows its first
// 50 items and a button for the rest. An "id" that is a URI links to its
// own lookup; "type" values are marked.
const OPEN_DEPTH = 2;
const SHOW_ITEMS = 50;

function text(cls, value) {
  const span = document.createElement("span");
  span.className = cls;
  span.textContent = value;
  return span;
}

function leaf(key, value) {
  if (value === null) return text("j-null", "null");
  if (typeof value === "string") {
    if ((key === "id" || key === "@id") && /^https?:\/\//.test(value)) {
      const a = document.createElement("a");
      a.className = "j-str j-link";
      a.href = `/records/show?q=${encodeURIComponent(value)}`;
      a.textContent = JSON.stringify(value);
      return a;
    }
    return text(key === "type" ? "j-str j-type" : "j-str", JSON.stringify(value));
  }
  if (typeof value === "number") return text("j-num", String(value));
  if (typeof value === "boolean") return text("j-bool", String(value));
  return text("j-str", JSON.stringify(value));
}

function node(key, value, depth) {
  const row = document.createElement("div");
  row.className = "j-row";
  const label = key === null ? null : text("j-key", typeof key === "number" ? `${key}` : JSON.stringify(key));
  if (value === null || typeof value !== "object") {
    if (label) row.append(label, text("j-colon", ": "));
    row.appendChild(leaf(key, value));
    return row;
  }
  const isArray = Array.isArray(value);
  const entries = isArray ? value.map((v, i) => [i, v]) : Object.entries(value);
  const details = document.createElement("details");
  details.open = depth < OPEN_DEPTH;
  const summary = document.createElement("summary");
  if (label) summary.append(label, text("j-colon", ": "));
  summary.appendChild(text("j-brace", isArray ? `[${entries.length}]` : `{${entries.length}}`));
  if (!isArray && typeof value.type === "string") summary.appendChild(text("j-type j-hint", ` ${value.type}`));
  details.appendChild(summary);
  const body = document.createElement("div");
  body.className = "j-body";
  const shown = isArray && entries.length > SHOW_ITEMS ? entries.slice(0, SHOW_ITEMS) : entries;
  for (const [k, v] of shown) body.appendChild(node(k, v, depth + 1));
  if (shown.length < entries.length) {
    const more = document.createElement("button");
    more.type = "button";
    more.className = "secondary j-more";
    more.textContent = `… ${entries.length - shown.length} more`;
    more.addEventListener("click", () => {
      for (const [k, v] of entries.slice(shown.length)) body.insertBefore(node(k, v, depth + 1), more);
      more.remove();
    });
    body.appendChild(more);
  }
  details.appendChild(body);
  row.appendChild(details);
  return row;
}

export function renderTree(value) {
  const root = document.createElement("div");
  root.className = "json-tree";
  root.appendChild(node(null, value, 0));
  return root;
}
