import requests
from typing import Optional
from pathlib import Path

def get_file_size(url: str) -> Optional[int]:
    """Get file size from server using HEAD request."""
    try:
        response = requests.head(url, allow_redirects=True)
        response.raise_for_status()
        return int(response.headers.get('content-length', 0))
    except (requests.exceptions.RequestException, ValueError):
        return None

def verify_files(local_path: str, remote_url: str) -> bool:
    """
    Verify if local and remote files match by comparing file sizes.
    """
    local_size = Path(local_path).stat().st_size
    remote_size = get_file_size(remote_url)
    
    if remote_size is not None:
        if local_size == remote_size:
            print(f"Files have same size: {local_size:,} bytes")
            return True
        else:
            print(f"Files differ in size: Local={local_size:,} bytes, Remote={remote_size:,} bytes")
            return False
    
    print("Could not verify file sizes.")
    return None