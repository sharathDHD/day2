# file_path/main3.py

import sqlite3
import logging
import ollama  # Ensure you have installed ollama-python (pip install ollama)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MODEL_NAME = "llama3.2"
DB_FILE = "scraped_data.db"


def rewrite_with_ollama(content: str) -> str:
    """
    Rewrite the provided content using the Ollama LLM.

    Args:
        content (str): Original markdown content.

    Returns:
        str: Rewritten content if successful; otherwise, an empty string.
    """
    prompt = f"Rewrite the following content in a better style:\n\n{content}"
    try:
        result = ollama.chat(MODEL_NAME, messages=[{"role": "user", "content": prompt}])
        # Check for expected response structure
        if "choices" in result and result["choices"]:
            return result["choices"][0]["message"]["content"].strip()
        elif "message" in result:
            return result["message"]["content"].strip()
        else:
            logger.error("No valid message returned from Ollama.")
            return ""
    except Exception as e:
        logger.error(f"Error rewriting content with Ollama: {e}", exc_info=True)
        return ""


def update_rewrite_in_db(conn: sqlite3.Connection, record_id: int, rewrite_content: str) -> None:
    """
    Update the 'rewrite' column for a specific record in the database.

    Args:
        conn (sqlite3.Connection): Active database connection.
        record_id (int): ID of the record to update.
        rewrite_content (str): Rewritten content.
    """
    update_sql = "UPDATE scraped_data SET rewrite = ? WHERE id = ?;"
    conn.execute(update_sql, (rewrite_content, record_id))
    conn.commit()


def process_records(conn: sqlite3.Connection) -> None:
    """
    Process database records with no rewritten content:
      - Fetch records where 'rewrite' is NULL or empty.
      - Rewrite the markdown content using Ollama.
      - Update the record with the rewritten text.
      
    Args:
        conn (sqlite3.Connection): Active database connection.
    """
    select_sql = "SELECT id, markdown FROM scraped_data WHERE rewrite IS NULL OR rewrite = '';"
    records = conn.execute(select_sql).fetchall()

    if not records:
        logger.info("No records to rewrite.")
        return

    logger.info(f"Found {len(records)} record(s) to rewrite.")
    for record_id, markdown in records:
        logger.info(f"Rewriting content for record ID: {record_id}")
        if not markdown:
            logger.warning(f"Record ID {record_id} has empty markdown content, skipping.")
            continue

        rewritten_content = rewrite_with_ollama(markdown)
        if rewritten_content:
            update_rewrite_in_db(conn, record_id, rewritten_content)
            logger.info(f"Updated record ID {record_id} with rewritten content.")
        else:
            logger.error(f"Failed to rewrite content for record ID {record_id}.")


def main() -> None:
    """
    Main function to connect to the database, process records needing rewriting, and update them.
    """
    with sqlite3.connect(DB_FILE) as conn:
        process_records(conn)


if __name__ == "__main__":
    main()
