// The record page: the stage tabs, and each record as a tree with a Raw
// toggle. Without this script the stages show one after another, as raw JSON.
import { renderTree } from "./jsontree.js";
import { setupTabs } from "./tabs.js";

setupTabs();

for (const view of document.querySelectorAll(".json-view")) {
  const data = view.querySelector("script.json-data");
  const raw = view.querySelector("pre.raw");
  if (!data) continue;
  const tree = renderTree(JSON.parse(data.textContent));
  view.insertBefore(tree, raw);
  raw.hidden = true;
}

for (const button of document.querySelectorAll(".raw-toggle")) {
  button.addEventListener("click", () => {
    const section = button.closest("section");
    for (const view of section.querySelectorAll(".json-view")) {
      const raw = view.querySelector("pre.raw");
      const tree = view.querySelector(".json-tree");
      raw.hidden = !raw.hidden;
      if (tree) tree.hidden = !raw.hidden;
    }
    button.textContent = button.textContent === "Raw" ? "Tree" : "Raw";
  });
}
