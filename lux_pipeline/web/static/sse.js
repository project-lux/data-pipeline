// Server-sent events: subscribe(url, {event: handler, ...}) parses each
// event's JSON data and calls its handler. EventSource reconnects by
// itself after a dropped connection; a "done" event closes the stream.
// Returns {stop()}.
export function subscribe(url, handlers) {
  const source = new EventSource(url);
  for (const [name, handler] of Object.entries(handlers)) {
    if (name === "error") continue;
    source.addEventListener(name, (event) => {
      let data = null;
      try { data = JSON.parse(event.data); } catch (e) { data = event.data; }
      handler(data);
      if (name === "done") source.close();
    });
  }
  if (!handlers.done) source.addEventListener("done", () => source.close());
  source.onerror = (event) => { if (handlers.error) handlers.error(event); };
  return { stop: () => source.close() };
}
