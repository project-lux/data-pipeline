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

    def fetch_data(self):
    """Transforms LC's ActivityStream into the format expected by process_items."""
    print(f"Fetching page: {self.page}")

    if self.cache_okay and self.page_cache is not None and self.page in self.page_cache:
        rec = self.page_cache[self.page]
        page = rec['data']
    else:
        page = self.fetch_json(self.page, 'page')
        if self.page_cache is not None and page is not None:
            if self.page in self.page_cache:
                rec = self.page_cache[self.page]
                cpage = rec['data']
                lc_time_key = 'published' if 'published' in page.get('orderedItems', [{}])[0] else 'endTime'

                if (
                    'orderedItems' in page
                    and 'orderedItems' in cpage
                    and lc_time_key in page['orderedItems'][0]
                    and lc_time_key in cpage['orderedItems'][0]
                    and page['orderedItems'][0][lc_time_key] == cpage['orderedItems'][0][lc_time_key]
                ):
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

    
    transformed_items = []
    for item in items:
        try:
            dt = item.get('published', None)
            if not dt:
                print(f"Skipping item with missing 'published' key: {item}")
                continue

            chg = item.get('type', '').lower()
            if chg == 'remove':
                chg = 'delete'
            elif chg == 'add':
                chg = 'create'

            # Extract the JSON URL
            json_url = None
            for link in item['object'].get('url', []):
                if link.get('mediaType') == 'application/json':
                    json_url = link['href']
                    break

            if not json_url:
                print(f"Skipping item with no valid JSON URL: {item}")
                continue

            # Skip records ending in '-781.json'
            if json_url.endswith('-781.json'):
                continue

            # Construct an AS-compatible item
            transformed_items.append({
                "type": chg,
                "endTime": dt,  
                "object": {"id": json_url}
            })

        except Exception as e:
            print(f"Error transforming item: {e}")
            continue

    return transformed_items
