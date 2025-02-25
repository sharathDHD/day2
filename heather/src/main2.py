# file_path/main.py

import sys
import sqlite3
import logging
import json
from scraper import run_job

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_table(conn: sqlite3.Connection) -> None:
    """
    Create the 'scraped_data' table if it does not exist.
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS scraped_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scrape_id TEXT UNIQUE,
        title TEXT,
        viewport TEXT,
        source_url TEXT,
        final_url TEXT,
        status_code INTEGER,
        markdown TEXT,
        html TEXT,
        meta_description TEXT,
        meta_keywords TEXT,
        open_graph TEXT,
        links TEXT,
        images TEXT,
        structured_data TEXT,
        headers TEXT,
        cookies TEXT,
        error TEXT,
        rewrite TEXT
    );
    """
    conn.execute(create_table_sql)
    conn.commit()

def store_result(conn: sqlite3.Connection, result: dict) -> None:
    """
    Store the scraping result into the SQLite database.
    
    Args:
        conn (sqlite3.Connection): The database connection.
        result (dict): The scraping result from run_job().
    """
    metadata = result.get("metadata", {})
    
    insert_sql = """
    INSERT OR REPLACE INTO scraped_data (
        scrape_id,
        title,
        viewport,
        source_url,
        final_url,
        status_code,
        markdown,
        html,
        meta_description,
        meta_keywords,
        open_graph,
        links,
        images,
        structured_data,
        headers,
        cookies,
        error
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    
    data = (
        result.get("scrape_id", ""),
        metadata.get("title", ""),
        metadata.get("viewport", ""),
        metadata.get("sourceURL", ""),
        metadata.get("url", ""),
        metadata.get("statusCode", None),
        result.get("markdown", ""),
        metadata.get("html", ""),
        metadata.get("meta_description", ""),
        metadata.get("meta_keywords", ""),
        json.dumps(metadata.get("open_graph", {})),
        json.dumps(metadata.get("links", [])),
        json.dumps(metadata.get("images", [])),
        json.dumps(metadata.get("structured_data", [])),
        json.dumps(metadata.get("headers", {})),
        json.dumps(metadata.get("cookies", {})),
        result.get("error", "")
    )
    
    conn.execute(insert_sql, data)
    conn.commit()

def main(url: str) -> None:
    """
    Scrape the provided URL and store the result in the SQLite database.
    
    Args:
        url (str): The URL to scrape.
    """
    # Connect to (or create) the SQLite database
    conn = sqlite3.connect("scraped_data.db")
    create_table(conn)
    
    logger.info(f"Scraping URL: {url}")
    result = run_job(url)
    
    store_result(conn, result)
    
    logger.info(f"Data stored with scrape_id: {result.get('scrape_id')}")
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_url = sys.argv[1]
    else:
        target_url = "https://www.github.com"  # Default URL if none provided.
    main(target_url)
