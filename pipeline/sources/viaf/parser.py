from pipeline.sources.utils import fetch_webpage
import re
from bs4 import BeautifulSoup

def parse_viaf(html_content: str) -> str:
    """Parse the first `resource` URL matching the specific pattern.
    
    Args:
        html_content (str): The HTML content of the VIAF data page to parse.
        
    Returns:
        str: The first resource URL matching the pattern 'https://viaf.org/viaf/data/viaf-NNNNNNNN-clusters.xml.gz',
             or None if no matching URL is found.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    files_available_section = soup.find('article', class_='data-files')
    if files_available_section:
        # Regex to match the desired pattern
        pattern = r"https://viaf\.org/viaf/data/viaf-\d+-clusters\.xml\.gz"
        # Find all <dl> elements with a 'resource' attribute
        dl_elements = files_available_section.find_all('dl', resource=True)
        for dl in dl_elements:
            resource_url = dl.get('resource')
            if resource_url and re.match(pattern, resource_url):
                return resource_url
    return None
