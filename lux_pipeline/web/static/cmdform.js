// A command form: the command line it will run, and whether it needs a
// typed confirmation, kept up to date as the form is filled in (POST to
// the form's data-preview; the server reads it as it reads the real
// submission). The build page also lists its steps (data-dry-run).

import { csrfToken } from "./app.js";

const form = document.querySelector("form.command");

async function send(url) {
  const resp = await fetch(url, {
    method: "POST", body: new FormData(form),
    headers: { "X-CSRF-Token": csrfToken(), "Accept": "application/json" },
  });
  return resp.json();
}

async function preview() {
  try {
    const data = await send(form.dataset.preview);
    const err = document.getElementById("cmd-error");
    document.getElementById("cmd-preview").textContent = data.command_line || "";
    err.textContent = data.error || "";
    err.hidden = !data.error;
    document.getElementById("cmd-confirm").hidden = !data.confirm;
    document.getElementById("cmd-confirm-reason").textContent = data.confirm || "";
  } catch (e) {
    /* the next change tries again */
  }
}

async function steps(event) {
  event.preventDefault();
  const list = document.getElementById("build-steps");
  const err = document.getElementById("build-steps-error");
  list.replaceChildren();
  err.hidden = true;
  try {
    const data = await send(form.dataset.dryRun);
    if (data.error) throw new Error(data.error);
    for (const s of data.steps) {
      const li = document.createElement("li");
      const code = document.createElement("code");
      code.textContent = s;
      li.append(code);
      list.append(li);
    }
  } catch (e) {
    err.textContent = e.message;
    err.hidden = false;
  }
}

if (form) {
  let timer = null;
  const later = (ev) => {
    if (ev.target && ev.target.name === "confirm") return;
    clearTimeout(timer);
    timer = setTimeout(preview, 250);
  };
  form.addEventListener("input", later);
  form.addEventListener("change", later);
  const button = document.getElementById("build-steps-button");
  if (button && form.dataset.dryRun) button.addEventListener("click", steps);
}
