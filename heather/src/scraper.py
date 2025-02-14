# file_path/scraper.py
import cloudscraper
from bs4 import BeautifulSoup
import uuid
import html2text

def run_job(url):
    scraper = cloudscraper.create_scraper()
    try:
        response = scraper.get(url)
        status_code = response.status_code
        # Convert HTML to Markdown using html2text
        h = html2text.HTML2Text()
        h.ignore_links = False
        markdown = h.handle(response.text)

        # Extract metadata using BeautifulSoup
        soup = BeautifulSoup(response.text, "html.parser")
        title = soup.title.string.strip() if soup.title and soup.title.string else "No Title"
        viewport_meta = soup.find("meta", attrs={"name": "viewport"})
        viewport = viewport_meta["content"] if viewport_meta and "content" in viewport_meta.attrs else "width=device-width, initial-scale=1"
        scrape_id = str(uuid.uuid4())

        result = {
            "markdown": markdown,
            "metadata": {
                "title": title,
                "viewport": viewport,
                "scrapeId": scrape_id,
                "sourceURL": url,
                "url": response.url,
                "statusCode": status_code,
            },
            "scrape_id": scrape_id
        }
        return result
    except Exception as e:
        return {
            "markdown": "",
            "metadata": {
                "title": "",
                "viewport": "",
                "scrapeId": "",
                "sourceURL": url,
                "url": "",
                "statusCode": None
            },
            "scrape_id": "",
            "error": str(e)
        }
