# file_path/db_config.py
import sqlite3
import psycopg2

# --- SQLite Functions ---
def connect_sqlite(db_path):
    # Allow SQLite connection objects to be shared across threads.
    conn = sqlite3.connect(db_path, check_same_thread=False)
    return conn


def get_tables_sqlite(conn):
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    return tables

def get_columns_sqlite(conn, table):
    cursor = conn.execute(f"PRAGMA table_info({table})")
    columns = [row[1] for row in cursor.fetchall()]
    return columns

def fetch_urls_sqlite(conn, table, url_column, batch_size=100, last_id=0):
    query = f"SELECT id, {url_column} FROM {table} WHERE id > ? ORDER BY id ASC LIMIT ?"
    cursor = conn.execute(query, (last_id, batch_size))
    rows = cursor.fetchall()
    return rows

def create_output_table_sqlite(conn, table_name):
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        url TEXT,
        response TEXT,
        status TEXT
    )
    """)
    conn.commit()

def store_output_sqlite(conn, table_name, job):
    conn.execute(f"INSERT INTO {table_name} (job_id, url, response, status) VALUES (?, ?, ?, ?)",
                 (job['id'], job['url'], job['response'].get('markdown', ''), job['status']))
    conn.commit()


# --- PostgreSQL Functions ---
def connect_postgres(host, port, database, user, password):
    conn = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
    return conn

def get_tables_postgres(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
    """)
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tables

def get_columns_postgres(conn, table):
    cursor = conn.cursor()
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (table,))
    columns = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return columns

def fetch_urls_postgres(conn, table, url_column, batch_size=100, last_id=0):
    cursor = conn.cursor()
    query = f"SELECT id, {url_column} FROM {table} WHERE id > %s ORDER BY id ASC LIMIT %s"
    cursor.execute(query, (last_id, batch_size))
    rows = cursor.fetchall()
    cursor.close()
    return rows

def create_output_table_postgres(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        job_id INTEGER,
        url TEXT,
        response TEXT,
        status TEXT
    )
    """)
    conn.commit()
    cursor.close()

def store_output_postgres(conn, table_name, job):
    cursor = conn.cursor()
    cursor.execute(f"INSERT INTO {table_name} (job_id, url, response, status) VALUES (%s, %s, %s, %s)",
                   (job['id'], job['url'], job['response'].get('markdown', ''), job['status']))
    conn.commit()
    cursor.close()
