// A form with data-stream="<events url>" fills the tables [data-section]
// on its page row by row from server-sent events (`row` with `section`
// and `cells`, then `done` with `totals`), instead of loading the page
// again with everything at once. Without this script the form does that.
import { subscribe } from "./sse.js";

for (const form of document.querySelectorAll("form[data-stream]")) {
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    const params = new URLSearchParams(new FormData(form));
    const status = document.querySelector("[data-stream-status]");
    const button = form.querySelector("button[type=submit]");
    for (const table of document.querySelectorAll("table[data-section]")) {
      table.tBodies[0].replaceChildren();
      const total = table.querySelector("[data-total]");
      if (total) total.textContent = "";
    }
    if (status) status.textContent = "Working…";
    if (button) button.disabled = true;
    let rows = 0;
    const started = Date.now();
    subscribe(`${form.dataset.stream}?${params}`, {
      row: (r) => {
        const table = document.querySelector(`table[data-section="${r.section}"]`);
        if (!table) return;
        const tr = document.createElement("tr");
        const headers = [...table.tHead.rows[0].cells];
        r.cells.forEach((text, i) => {
          const td = document.createElement("td");
          if (headers[i] && headers[i].classList.contains("num")) td.className = "num";
          td.textContent = text;
          tr.appendChild(td);
        });
        table.tBodies[0].appendChild(tr);
        rows += 1;
        if (status) status.textContent = `Working… ${rows} so far`;
      },
      done: (d) => {
        for (const [section, text] of Object.entries(d.totals || {})) {
          const total = document.querySelector(`table[data-section="${section}"] [data-total]`);
          if (total) total.textContent = text;
        }
        const secs = Math.round((Date.now() - started) / 1000);
        let msg = `Done: ${rows} row(s) in ${secs}s.`;
        if (d.skipped && d.skipped.length) msg += ` Not in Postgres (skipped): ${d.skipped.join(", ")}.`;
        if (status) status.textContent = msg;
        if (button) button.disabled = false;
      },
      failed: (d) => {
        if (status) status.textContent = `Stopped: ${d.error}`;
        if (button) button.disabled = false;
      },
    });
  });
}
