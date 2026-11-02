// Every page: the header clock, the "running" badge, and fetch() with the
// CSRF token. No inline scripts anywhere: pages load modules from /static.

export function csrfToken() {
  const meta = document.querySelector('meta[name="csrf-token"]');
  return meta ? meta.content : "";
}

// fetch() that sends the CSRF token, as every POST must.
export function post(url, body) {
  return fetch(url, {
    method: "POST",
    headers: { "X-CSRF-Token": csrfToken(), "Content-Type": "application/json", "Accept": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

function tickClock() {
  const el = document.getElementById("clock");
  if (el) el.textContent = new Date().toLocaleTimeString([], { hour12: false });
}

async function pollRunning() {
  const badge = document.getElementById("running-badge");
  if (!badge) return;
  try {
    const resp = await fetch("/api/dashboard/running", { headers: { "Accept": "application/json" } });
    if (!resp.ok) { badge.hidden = true; return; }
    const runs = await resp.json();
    badge.hidden = runs.length === 0;
    if (runs.length) {
      badge.textContent = runs.length === 1 ? `running: ${runs[0].command}` : `${runs.length} running`;
      badge.title = runs.map((r) => r.prefix).join("\n");
      badge.href = runs.length === 1 && runs[0].rel ? `/runs/${runs[0].rel}` : "/runs?state=running";
    }
  } catch (e) {
    badge.hidden = true;
  }
}

tickClock();
setInterval(tickClock, 1000);
pollRunning();
setInterval(pollRunning, 5000);
