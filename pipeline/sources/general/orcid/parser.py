from bs4 import BeautifulSoup
import re
from pipeline.sources.utils import fetch_webpage

def get_yearly_url(html_content, year):
    """
    Parses HTML to find the URL for the ORCID Public Data File for the given year.

    Args:
        html_content (str): The HTML content of the base webpage.
        year (int): The year to search for in the link text.

    Returns:
        str: The URL of the yearly ORCID Public Data File page, or None if not found.
    """
    soup = BeautifulSoup(html_content, "html.parser")
    link_text_pattern = re.compile(rf"{year} ORCID Public Data File", re.IGNORECASE)
    for link in soup.find_all("a", href=True):
        if link_text_pattern.search(link.get_text(strip=True)):
            return link["href"]  # Return the href of the matching link
    return None

def get_download_link(html_content, year):
    """
    Parses HTML to find the direct download link for the ORCID Public Data File.

    Args:
        html_content (str): The HTML content of the yearly webpage.
        year (int): The year to validate the expected file name.

    Returns:
        str: The download link for the file, or None if not found.
    """
    #current_year = 2024
    expected_file_name = f"ORCID_{year}_10_summaries.tar.gz"

    soup = BeautifulSoup(html_content, "html.parser")
    link_pattern = re.compile(r'https://orcid\.figshare\.com/ndownloader/files/\d+')
    download_links = link_pattern.findall(html_content)

    # Validate the links against the expected file name
    for link in download_links:
        # Check if the expected file name is mentioned in the HTML
        if expected_file_name in html_content:
            # print(f"Download link for {expected_file_name}: {link}")
            return link
    else:
        print(f"Download link for {expected_file_name} not found in the HTML.")

def download_orcid(year=2024):
    base_url = "https://info.orcid.org/documentation/integration-guide/working-with-bulk-data/"
    html_content = fetch_webpage(base_url)
    if not html_content:
        print("Failed to fetch the base webpage content.")

    yearly_url = get_yearly_url(html_content, year)
    if not yearly_url:
        print(f"No valid yearly URL found for {year}.")

    yearly_html = fetch_webpage(yearly_url)
    if not yearly_html:
        print(f"Failed to fetch the yearly webpage at {yearly_url}.")

    download_link = get_download_link(yearly_html, year)
    if not download_link:
        print(f"No valid download link found for {year}.")

    urls = [download_link]
    return urls

