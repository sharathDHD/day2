# file_path/scraper.py

import uuid
import logging
import cloudscraper
from bs4 import BeautifulSoup
import html2text
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def convert_html_to_markdown(html: str) -> str:
    """
    Convert HTML content to Markdown using html2text.
    
    Args:
        html (str): The HTML content to convert.
        
    Returns:
        str: The converted Markdown text.
    """
    converter = html2text.HTML2Text()
    converter.ignore_links = False
    return converter.handle(html)

def extract_metadata(
    html: str,
    original_url: str,
    final_url: str,
    status_code: int,
    scrape_id: str,
    headers: Dict[str, Any],
    cookies: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Extract metadata from HTML content using BeautifulSoup, including additional info such as:
      - Meta description and keywords.
      - Open Graph tags.
      - All links and images.
      - Structured data (JSON-LD).
      - HTTP headers and cookies.
    
    Args:
        html (str): The HTML content to parse.
        original_url (str): The URL originally requested.
        final_url (str): The final URL after redirects.
        status_code (int): HTTP status code.
        scrape_id (str): Unique identifier for the scrape.
        headers (dict): HTTP response headers.
        cookies (dict): Cookies from the response.
        
    Returns:
        dict: A dictionary with extended metadata.
    """
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.string.strip() if soup.title and soup.title.string else "No Title"
    
    viewport_meta = soup.find("meta", attrs={"name": "viewport"})
    viewport = (
        viewport_meta["content"]
        if viewport_meta and viewport_meta.has_attr("content")
        else "width=device-width, initial-scale=1"
    )
    
    # Meta description
    desc_meta = soup.find("meta", attrs={"name": "description"})
    meta_description = desc_meta["content"].strip() if desc_meta and desc_meta.has_attr("content") else ""
    
    # Meta keywords
    keywords_meta = soup.find("meta", attrs={"name": "keywords"})
    meta_keywords = keywords_meta["content"].strip() if keywords_meta and keywords_meta.has_attr("content") else ""
    
    # Open Graph tags extraction
    og_tags = {}
    for meta in soup.find_all("meta", attrs={"property": True}):
        prop = meta.get("property", "")
        if prop.startswith("og:"):
            og_tags[prop] = meta.get("content", "")
    
    # Extract all links (anchor href values)
    links = [a.get("href") for a in soup.find_all("a", href=True)]
    
    # Extract all image sources
    images = [img.get("src") for img in soup.find_all("img", src=True)]
    
    # Extract structured data (JSON-LD scripts)
    structured_data = []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            if script.string:
                structured_data.append(script.string.strip())
        except Exception:
            pass
    
    return {
        "title": title,
        "viewport": viewport,
        "scrapeId": scrape_id,
        "sourceURL": original_url,
        "url": final_url,
        "statusCode": status_code,
        "html": html,
        "meta_description": meta_description,
        "meta_keywords": meta_keywords,
        "open_graph": og_tags,
        "links": links,
        "images": images,
        "structured_data": structured_data,
        "headers": headers,
        "cookies": cookies
    }

def run_job(url: str) -> Dict[str, Any]:
    """
    Scrape the given URL and return its Markdown content along with extended metadata.
    
    Args:
        url (str): The URL to scrape.
        
    Returns:
        dict: Contains the Markdown content, metadata, and scrape_id.
              On error, includes an error message.
    """
    scraper = cloudscraper.create_scraper()
    scrape_id = str(uuid.uuid4())
    
    try:
        response = scraper.get(url, timeout=10)
        response.raise_for_status()  # Raise error for HTTP errors
        
        html_content = response.text
        markdown = convert_html_to_markdown(html_content)
        
        # Gather additional response data
        headers = dict(response.headers)
        cookies = response.cookies.get_dict()
        
        metadata = extract_metadata(
            html_content,
            url,
            response.url,
            response.status_code,
            scrape_id,
            headers,
            cookies
        )
        
        return {
            "markdown": markdown,
            "metadata": metadata,
            "scrape_id": scrape_id,
        }
        
    except Exception as e:
        logger.error(f"Error scraping {url}: {e}", exc_info=True)
        return {
            "markdown": "",
            "metadata": {
                "title": "",
                "viewport": "",
                "scrapeId": "",
                "sourceURL": url,
                "url": "",
                "statusCode": None,
                "html": "",
                "meta_description": "",
                "meta_keywords": "",
                "open_graph": {},
                "links": [],
                "images": [],
                "structured_data": [],
                "headers": {},
                "cookies": {}
            },
            "scrape_id": "",
            "error": str(e),
        }

if __name__ == "__main__":
    # For quick testing purposes
    test_url = "https://www.example.com"
    result = run_job(test_url)
    logger.info(f"Scraping result: {result}")
