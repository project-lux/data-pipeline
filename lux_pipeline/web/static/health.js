// The health page: while the checks run, rows arrive over server-sent
// events as each finishes. Built with DOM nodes, never innerHTML: details
// are text from the checks.
import { subscribe } from "./sse.js";

const table = document.getElementById("health-table");
const status = document.getElementById("health-status");

function row(result) {
  const tr = document.createElement("tr");
  const label = document.createElement("td");
  label.textContent = result.label;
  const state = document.createElement("td");
  const badge = document.createElement("span");
  badge.className = `badge state-${(result.state || "unknown").toLowerCase()}`;
  badge.textContent = result.state;
  state.appendChild(badge);
  const detail = document.createElement("td");
  detail.className = "detail";
  detail.textContent = result.detail || "";
  tr.append(label, state, detail);
  return tr;
}

if (table && table.dataset.running) {
  const body = table.tBodies[0];
  body.replaceChildren();
  subscribe("/api/health/events", {
    result: (r) => body.appendChild(row(r)),
    done: (h) => {
      const fails = h.results.filter((r) => r.state === "FAIL").length;
      if (status) status.textContent = `Checked: ${h.results.length} checks, ${fails} failed.`;
      const button = document.querySelector("form button[type=submit]");
      if (button) { button.disabled = false; button.textContent = "Run the checks"; }
    },
  });
}
