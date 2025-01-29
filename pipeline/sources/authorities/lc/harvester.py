from pipeline.process.base.harvester import ASHarvester
import sys

class LCHarvester(ASHarvester):

    def __init__(self, config):
        super().__init__(config)

    def fetch_collection(self, uri):
        try:
            coll = self.fetch_json(uri, 'collection')
            self.page = coll.get('first', "")
        except Exception as e:
            self.page = None
            print(f"Failed to get first page from collection {uri}: {e}")

    def fetch_page(self):
        print(f"Fetching page: {self.page}")

        # Handle caching logic
        if self.cache_okay and self.page_cache is not None and self.page in self.page_cache:
            rec = self.page_cache[self.page]
            page = rec['data']
        else:
            page = self.fetch_json(self.page, 'page')
            if self.page_cache is not None and page is not None:
                if self.page_cache is not None and page is not None:
                    if self.page in self.page_cache:
                        rec = self.page_cache[self.page]
                        cpage = rec['data']
                        if 'orderedItems' in page and 'orderedItems' in cpage and page['orderedItems'][0]['endTime'] == cpage['orderedItems'][0]['endTime']:
                            self.cache_okay = True
                        else:
                            self.page_cache[self.page] = page
                    else:
                        self.page_cache[self.page] = page

        try:
            items = page.get("orderedItems", [])
        except KeyError:
            print(f"Failed to get items from page {self.page}")
            items = []

        try:
            # LC uses 'next' for pagination
            next_page = page.get('next', None)
            self.page = next_page if next_page and next_page != self.page else None
        except KeyError:
            self.page = None

        sys.stdout.write('P')
        sys.stdout.flush()
        return items

    def process_items(self, items, refsonly=False):
        for item in items:
            try:
                # Use `published` instead of `endTime` for LC
                dt = item.get('published', None)
                if not dt:
                    print(f"Missing 'published' key for item: {item}")
                    continue
                elif dt < self.last_harvest:
                    # We're done with the stream, not just this page
                    self.page = None
                    return

                # Determine the change type
                chg = None
                try:
                    if isinstance(item['type'], str):
                        chg = item['type'].lower()
                    elif isinstance(item['type'], list) and len(item['type']) > 0:
                        chg = item['type'][0].lower()
                    else:
                        raise ValueError(f"Unexpected 'type' format: {item['type']}")

                    # Map LC-specific types to standard change types
                    if chg == 'remove':
                        chg = 'delete'
                    elif chg == 'add':
                        chg = 'create'
                except Exception as e:
                    print(f"Error determining change type for item: {e}")
                    continue

                # Extract the JSON URL from the 'url' array
                url = None
                for link in item['object'].get('url', []):
                    if link.get('mediaType') == 'application/json':
                        url = link['href']
                        break

                if not url:
                    print(f"No valid JSON URL for item: {item}")
                    continue

                # Skip URLs ending with '-781'
                if url.endswith('-781.json'):
                    #print(f"Skipping item with URL ending in '-781': {url}")
                    continue

                # Generate the identifier
                ident = url.replace(self.namespace, "")

                # Check if the item has already been processed
                if ident in self.seen or ident in self.deleted:
                    continue
                self.seen[ident] = 1

                # Handle deletions
                if chg == 'delete':
                    yield (chg, ident, {}, "")
                    sys.stdout.write('X')
                    sys.stdout.flush()
                    continue

                # Yield processed items
                yield (chg, ident, {"url": url, "dt": dt}, dt)
                sys.stdout.write('.')
                sys.stdout.flush()

            except Exception as e:
                print(f"Error processing item: {e}")
                continue



