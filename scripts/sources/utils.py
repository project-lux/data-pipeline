import requests

def fetch_webpage(url: str) -> str:
    """Fetch the webpage content from the given URL.
    
    Args:
        url (str): The URL of the webpage to fetch.
        
    Returns:
        str: The text content of the webpage if successful, None if the request fails.
        
    Raises:
        requests.RequestException: If there is an error making the HTTP request.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching the URL '{url}': {e}")
        return None