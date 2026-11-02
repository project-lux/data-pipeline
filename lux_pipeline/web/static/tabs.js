// Tabs: buttons [data-tab=name] show the [data-panel=name] section and hide
// the others, remembering the choice in ?tab= so a reload keeps it. Without
// this script every panel shows, one after another.
export function setupTabs(fallback) {
  const tabs = [...document.querySelectorAll("[data-tab]")];
  if (!tabs.length) return null;
  function show(name) {
    for (const t of tabs) t.setAttribute("aria-selected", String(t.dataset.tab === name));
    for (const p of document.querySelectorAll("[data-panel]")) p.hidden = p.dataset.panel !== name;
    const url = new URL(window.location);
    url.searchParams.set("tab", name);
    url.searchParams.delete("flash");
    history.replaceState(null, "", url);
  }
  const selected = tabs.find((t) => t.getAttribute("aria-selected") === "true" && !t.disabled);
  show(selected ? selected.dataset.tab : fallback || tabs.find((t) => !t.disabled).dataset.tab);
  for (const t of tabs) t.addEventListener("click", () => { if (!t.disabled) show(t.dataset.tab); });
  return show;
}
