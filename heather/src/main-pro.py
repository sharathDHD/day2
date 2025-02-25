import flet as ft
import threading
import json
import time
import asyncio
from scraper import run_job
from formatter import parse_file
import db_config

# Global job list: each job is a dict with keys: id, url, status, response, etc.
jobs = []

def add_job(url, db_config_info=None):
    job = {
        "id": len(jobs) + 1,
        "url": url,
        "status": "in queue",
        "response": {},
    }
    if db_config_info:
        job["db_config"] = db_config_info
    jobs.append(job)
    return job

# Global DB connection variables and last fetched IDs
sqlite_conn = None
sqlite_last_id = 0
pg_conn = None
pg_last_id = 0

def run_scraper(job):
    # Runs in a background thread.
    job["status"] = "in progress"
    result = run_job(job["url"])
    job["response"] = result
    job["status"] = "completed" if result.get("metadata", {}).get("statusCode") == 200 else "error"
    # If the job came from a DB source, store the output in the output table.
    if job.get("db_config"):
        db_type = job["db_config"]["type"]
        output_table = job["db_config"].get("output_table", "scrape_output")
        if db_type == "sqlite" and sqlite_conn:
            db_config.create_output_table_sqlite(sqlite_conn, output_table)
            db_config.store_output_sqlite(sqlite_conn, output_table, job)
        elif db_type == "postgres" and pg_conn:
            db_config.create_output_table_postgres(pg_conn, output_table)
            db_config.store_output_postgres(pg_conn, output_table, job)
    # UI updates are handled by the periodic update in the async main loop.

async def main(page: ft.Page):
    page.title = "Flet Scraper App"

    # ----------------------------
    # Scrape Tab (manual URL entry)
    # ----------------------------
    url_field = ft.TextField(label="Enter URL", width=400)
    def on_add_job(e):
        url = url_field.value
        if url:
            new_job = add_job(url)
            threading.Thread(target=run_scraper, args=(new_job,), daemon=True).start()
            url_field.value = ""
    add_job_button = ft.ElevatedButton(text="Add Job", on_click=on_add_job)
    file_picker = ft.FilePicker(on_result=lambda e: on_file_upload_result(e))
    page.overlay.append(file_picker)
    file_upload_button = ft.ElevatedButton(text="Upload File", on_click=lambda e: file_picker.pick_files())
    scrape_tab_content = ft.Column(
        [
            ft.Row([url_field, add_job_button, file_upload_button]),
            ft.Text("Use this tab to add new scraping jobs manually.")
        ],
        scroll=True
    )

    def on_file_upload_result(e: ft.FilePickerResultEvent):
        if e.files:
            urls = parse_file(e.files[0].path)
            for url in urls:
                new_job = add_job(url)
                threading.Thread(target=run_scraper, args=(new_job,), daemon=True).start()

    # ----------------------------
    # Jobs List Tab
    # ----------------------------
    job_list = ft.ListView(expand=True, width=300)
    job_detail = ft.Column([ft.Text("Select a job to view details")], scroll=True, expand=True)
    def update_job_list():
        job_list.controls.clear()
        for job in jobs:
            job_item = ft.ListTile(
                title=ft.Text(f"Job {job['id']}: {job['url']}"),
                subtitle=ft.Text(f"Status: {job['status']}"),
                on_click=lambda e, job=job: show_job_detail(job)
            )
            job_list.controls.append(job_item)

    def show_job_detail(job):
        result = job["response"]
        json_response = json.dumps(result, indent=2) if result else "{}"
        markdown_text = result.get("markdown", "No Markdown available.") if result else "No Markdown available."
        header = ft.Column(
            [
                ft.Text(result.get("metadata", {}).get("title", "No Title"), weight="bold") if result else ft.Text("No Title"),
                ft.Text(result.get("metadata", {}).get("sourceURL", job["url"]), color="blue", size=12) if result else ft.Text(job["url"])
            ]
        )
        detail_row = ft.Row(
            [
                ft.Column(
                    [
                        ft.Text("Markdown", weight="bold"),
                        ft.Markdown(markdown_text, expand=True)
                    ],
                    expand=True
                ),
                ft.VerticalDivider(),
                ft.Column(
                    [
                        ft.Text("JSON Response", weight="bold"),
                        ft.Text(json_response, expand=True, font_family="monospace")
                    ],
                    expand=True
                ),
            ],
            expand=True
        )
        detail_view = ft.Column([header, ft.Divider(), detail_row], expand=True)
        job_detail.controls = [detail_view]

    # ----------------------------
    # DB Config Tab Components
    # ----------------------------
    db_type_dropdown = ft.Dropdown(
        label="Select DB Type",
        options=[ft.dropdown.Option("SQLite"), ft.dropdown.Option("PostgreSQL")],
        value="SQLite"
    )

    # === SQLite UI Elements ===
    sqlite_path_field = ft.TextField(label="SQLite DB Path", width=400)
    sqlite_load_tables_button = ft.ElevatedButton(text="Load Tables", on_click=lambda e: load_sqlite_tables(e))
    sqlite_table_dropdown = ft.Dropdown(label="Select Table", width=400)
    sqlite_column_dropdown = ft.Dropdown(label="Select URL Column", width=400)
    sqlite_output_field = ft.TextField(label="Output Table Name", value="scrape_output", width=400)
    sqlite_batch_field = ft.TextField(label="Batch Size", value="100", width=100)
    sqlite_poll_field = ft.TextField(label="Poll Interval (sec)", value="5", width=100)
    sqlite_add_urls_button = ft.ElevatedButton(text="Add URLs", on_click=lambda e: add_urls_sqlite(e))
    sqlite_start_polling_button = ft.ElevatedButton(text="Start Polling", on_click=lambda e: start_polling_sqlite(e))
    sqlite_container = ft.Column(
        [
            sqlite_path_field,
            sqlite_load_tables_button,
            sqlite_table_dropdown,
            sqlite_column_dropdown,
            sqlite_output_field,
            ft.Row([sqlite_batch_field, sqlite_poll_field]),
            ft.Row([sqlite_add_urls_button, sqlite_start_polling_button])
        ],
        visible=True
    )

    # === PostgreSQL UI Elements ===
    pg_host_field = ft.TextField(label="Host", value="localhost", width=200)
    pg_port_field = ft.TextField(label="Port", value="5432", width=100)
    pg_db_field = ft.TextField(label="Database", width=200)
    pg_user_field = ft.TextField(label="User", width=200)
    pg_password_field = ft.TextField(label="Password", width=200, password=True)
    pg_connect_button = ft.ElevatedButton(text="Connect", on_click=lambda e: connect_pg(e))
    pg_table_dropdown = ft.Dropdown(label="Select Table", width=400)
    pg_column_dropdown = ft.Dropdown(label="Select URL Column", width=400)
    pg_output_field = ft.TextField(label="Output Table Name", value="scrape_output", width=400)
    pg_batch_field = ft.TextField(label="Batch Size", value="100", width=100)
    pg_poll_field = ft.TextField(label="Poll Interval (sec)", value="5", width=100)
    pg_add_urls_button = ft.ElevatedButton(text="Add URLs", on_click=lambda e: add_urls_pg(e))
    pg_start_polling_button = ft.ElevatedButton(text="Start Polling", on_click=lambda e: start_polling_pg(e))
    postgres_container = ft.Column(
        [
            ft.Row([pg_host_field, pg_port_field]),
            pg_db_field,
            ft.Row([pg_user_field, pg_password_field]),
            pg_connect_button,
            pg_table_dropdown,
            pg_column_dropdown,
            pg_output_field,
            ft.Row([pg_batch_field, pg_poll_field]),
            ft.Row([pg_add_urls_button, pg_start_polling_button])
        ],
        visible=False
    )

    def on_db_type_change(e):
        if db_type_dropdown.value == "SQLite":
            sqlite_container.visible = True
            postgres_container.visible = False
        else:
            sqlite_container.visible = False
            postgres_container.visible = True

    db_type_dropdown.on_change = on_db_type_change

    # --- SQLite Functions ---
    def load_sqlite_tables(e):
        global sqlite_conn
        try:
            sqlite_conn = db_config.connect_sqlite(sqlite_path_field.value)
            tables = db_config.get_tables_sqlite(sqlite_conn)
            sqlite_table_dropdown.options = [ft.dropdown.Option(t) for t in tables]
            sqlite_table_dropdown.value = tables[0] if tables else None
            on_sqlite_table_changed(None)
        except Exception as ex:
            print(f"Error connecting to SQLite: {ex}")

    def on_sqlite_table_changed(e):
        if sqlite_conn and sqlite_table_dropdown.value:
            columns = db_config.get_columns_sqlite(sqlite_conn, sqlite_table_dropdown.value)
            sqlite_column_dropdown.options = [ft.dropdown.Option(c) for c in columns]
            sqlite_column_dropdown.value = columns[0] if columns else None

    sqlite_table_dropdown.on_change = on_sqlite_table_changed

    def add_urls_sqlite(e):
        global sqlite_last_id
        if not sqlite_conn:
            print("SQLite not connected")
            return
        table = sqlite_table_dropdown.value
        column = sqlite_column_dropdown.value
        try:
            batch_size = int(sqlite_batch_field.value)
        except:
            batch_size = 100
        rows = db_config.fetch_urls_sqlite(sqlite_conn, table, column, batch_size, sqlite_last_id)
        if rows:
            sqlite_last_id = max(row[0] for row in rows)
            for row in rows:
                url = row[1]
                new_job = add_job(url, {"type": "sqlite", "output_table": sqlite_output_field.value})
                threading.Thread(target=run_scraper, args=(new_job,), daemon=True).start()

    def start_polling_sqlite(e):
        try:
            poll_interval = int(sqlite_poll_field.value)
        except:
            poll_interval = 5
        def poll():
            while True:
                add_urls_sqlite(None)
                time.sleep(poll_interval)
        threading.Thread(target=poll, daemon=True).start()

    # --- PostgreSQL Functions ---
    def connect_pg(e):
        global pg_conn
        try:
            pg_conn = db_config.connect_postgres(
                pg_host_field.value,
                pg_port_field.value,
                pg_db_field.value,
                pg_user_field.value,
                pg_password_field.value
            )
            tables = db_config.get_tables_postgres(pg_conn)
            pg_table_dropdown.options = [ft.dropdown.Option(t) for t in tables]
            pg_table_dropdown.value = tables[0] if tables else None
            on_pg_table_changed(None)
        except Exception as ex:
            print(f"Error connecting to PostgreSQL: {ex}")

    def on_pg_table_changed(e):
        if pg_conn and pg_table_dropdown.value:
            columns = db_config.get_columns_postgres(pg_conn, pg_table_dropdown.value)
            pg_column_dropdown.options = [ft.dropdown.Option(c) for c in columns]
            pg_column_dropdown.value = columns[0] if columns else None

    pg_table_dropdown.on_change = on_pg_table_changed

    def add_urls_pg(e):
        global pg_last_id
        if not pg_conn:
            print("PostgreSQL not connected")
            return
        table = pg_table_dropdown.value
        column = pg_column_dropdown.value
        try:
            batch_size = int(pg_batch_field.value)
        except:
            batch_size = 100
        rows = db_config.fetch_urls_postgres(pg_conn, table, column, batch_size, pg_last_id)
        if rows:
            pg_last_id = max(row[0] for row in rows)
            for row in rows:
                url = row[1]
                new_job = add_job(url, {"type": "postgres", "output_table": pg_output_field.value})
                threading.Thread(target=run_scraper, args=(new_job,), daemon=True).start()

    def start_polling_pg(e):
        try:
            poll_interval = int(pg_poll_field.value)
        except:
            poll_interval = 5
        def poll():
            while True:
                add_urls_pg(None)
                time.sleep(poll_interval)
        threading.Thread(target=poll, daemon=True).start()

    db_config_tab_content = ft.Column(
        [
            db_type_dropdown,
            sqlite_container,
            postgres_container
        ],
        scroll=True
    )

    # ----------------------------
    # Tabs Setup
    # ----------------------------
    tabs = ft.Tabs(
        tabs=[
            ft.Tab(text="Scrape", content=scrape_tab_content),
            ft.Tab(text="Jobs List", content=ft.Row([job_list, ft.VerticalDivider(), job_detail], expand=True)),
            ft.Tab(text="DB Config", content=db_config_tab_content),
        ],
        expand=True
    )
    page.add(tabs)

    # Asynchronous periodic update to refresh the UI on the main thread.
    async def periodic_update():
        while True:
            update_job_list()
            page.update()
            await asyncio.sleep(1)

    # Schedule the periodic update on the active event loop.
    asyncio.create_task(periodic_update())

# Run the async main function.
ft.app(target=main)
