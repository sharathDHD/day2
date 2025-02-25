import flet as ft
import threading
import json
import time
import asyncio
from scraper import run_job
from formatter import parse_file
import db_config


class ScraperApp:
    def __init__(self, page: ft.Page):
        self.page = page
        self.page.title = "Flet Scraper App"
        
        # Define custom dark and light themes.
        self.dark_theme = ft.Theme(
            color_scheme_seed=ft.Colors.DEEP_PURPLE,
            card_color=ft.Colors.BLUE_GREY_900,
        )
        self.light_theme = ft.Theme(
            color_scheme_seed=ft.Colors.DEEP_PURPLE,
            card_color=ft.Colors.WHITE,
        )
        # Set initial theme to dark.
        self.page.theme = self.dark_theme

        self.jobs = []
        self.sqlite_conn = None
        self.sqlite_last_id = 0
        self.pg_conn = None
        self.pg_last_id = 0

        # Build UI components.
        self._create_scrape_tab()
        self._create_jobs_tab()
        self._create_db_config_tab()
        self._create_settings_tab()

        self.tabs = ft.Tabs(
            tabs=[
                ft.Tab(text="Scrape", icon=ft.Icons.ADD_BOX, content=self.scrape_tab),
                ft.Tab(
                    text="Jobs List",
                    icon=ft.Icons.LIST,
                    content=ft.Row(
                        [self.job_list, ft.VerticalDivider(), self.job_detail],
                        expand=True,
                    ),
                ),
                ft.Tab(text="DB Config", icon=ft.Icons.STORAGE, content=self.db_config_tab),
                ft.Tab(text="Settings", icon=ft.Icons.SETTINGS, content=self.settings_tab),
            ],
            expand=True,
        )
        self.page.add(self.tabs)

    def get_scrape_bg(self):
        """Return a background color for the Scrape tab container based on theme."""
        if self.page.theme_mode == ft.ThemeMode.DARK:
            return ft.Colors.BLUE_700
        else:
            return ft.Colors.BLUE_200

    # ─── SCRAPE TAB ───────────────────────────────────────────────
    def _create_scrape_tab(self):
        self.url_field = ft.TextField(label="Enter URL", width=400, border="underline")
        self.add_job_button = ft.ElevatedButton(
            text="Add Job", icon=ft.Icons.ADD, on_click=self._on_add_job
        )
        self.file_picker = ft.FilePicker(on_result=self._on_file_upload_result)
        self.page.overlay.append(self.file_picker)
        self.upload_button = ft.ElevatedButton(
            text="Upload File", icon=ft.Icons.UPLOAD_FILE, on_click=lambda e: self.file_picker.pick_files()
        )
        self.scrape_tab = ft.Container(
            content=ft.Column(
                [
                    ft.Text("Add a New Scraping Job", size=20, weight="bold"),
                    ft.Row(
                        [self.url_field, self.add_job_button, self.upload_button],
                        alignment="center",
                    ),
                    ft.Divider(height=20),
                    ft.Text("Enter a URL manually or upload a file containing URLs."),
                ],
                alignment="center",
                spacing=20,
            ),
            padding=20,
            bgcolor=self.get_scrape_bg(),
            border_radius=10,
            alignment=ft.alignment.center,
            expand=True,
        )

    def _on_add_job(self, e):
        url = self.url_field.value.strip()
        if url:
            new_job = self._add_job(url)
            threading.Thread(target=self._run_scraper, args=(new_job,), daemon=True).start()
            self.url_field.value = ""
            self._show_snack("Job added successfully!")
        else:
            self._show_snack("Please enter a valid URL.")
        self.page.update()

    def _on_file_upload_result(self, e: ft.FilePickerResultEvent):
        if e.files:
            urls = parse_file(e.files[0].path)
            for url in urls:
                new_job = self._add_job(url)
                threading.Thread(target=self._run_scraper, args=(new_job,), daemon=True).start()
            self._show_snack(f"{len(urls)} jobs added from file.")
        self.page.update()

    def _add_job(self, url, db_config_info=None):
        job = {
            "id": len(self.jobs) + 1,
            "url": url,
            "status": "in queue",
            "response": {},
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        if db_config_info:
            job["db_config"] = db_config_info
        self.jobs.append(job)
        return job

    def _run_scraper(self, job):
        job["status"] = "in progress"
        result = run_job(job["url"])
        job["response"] = result
        job["status"] = (
            "completed" if result.get("metadata", {}).get("statusCode") == 200 else "error"
        )
        if job.get("db_config"):
            db_type = job["db_config"]["type"]
            output_table = job["db_config"].get("output_table", "scrape_output")
            try:
                if db_type == "sqlite" and self.sqlite_conn:
                    db_config.create_output_table_sqlite(self.sqlite_conn, output_table)
                    db_config.store_output_sqlite(self.sqlite_conn, output_table, job)
                elif db_type == "postgres" and self.pg_conn:
                    db_config.create_output_table_postgres(self.pg_conn, output_table)
                    db_config.store_output_postgres(self.pg_conn, output_table, job)
            except Exception as ex:
                print(f"Error storing job output: {ex}")
        self.page.update()

    # ─── JOBS TAB ───────────────────────────────────────────────
    def _create_jobs_tab(self):
        self.job_list = ft.ListView(expand=True, width=350, padding=10, spacing=10)
        self.job_detail = ft.Container(
            content=ft.Column(
                [ft.Text("Select a job to view details", weight="bold")],
                alignment="center",
            ),
            expand=True,
            padding=20,
            border=ft.border.all(1, ft.Colors.GREY_300),
            border_radius=10,
        )

    def _update_job_list(self):
        self.job_list.controls.clear()
        for job in self.jobs:
            if job["status"] == "completed":
                card_color = ft.Colors.GREEN
            elif job["status"] == "error":
                card_color = ft.Colors.RED
            else:
                card_color = ft.Colors.YELLOW
            job_card = ft.Card(
                content=ft.Container(
                    content=ft.Column(
                        [
                            ft.Text(f"Job {job['id']}", weight="bold"),
                            ft.Text(job["url"], size=12, color=ft.Colors.BLUE),
                            ft.Text(f"Status: {job['status']}", size=12),
                        ],
                        spacing=5,
                    ),
                    padding=10,
                    bgcolor=card_color,
                    border_radius=5,
                    on_click=lambda e, job=job: self._show_job_detail(job),
                )
            )
            self.job_list.controls.append(job_card)

    def _show_job_detail(self, job):
        result = job.get("response", {})
        json_response = json.dumps(result, indent=2) if result else "{}"
        markdown_text = (
            result.get("markdown", "No Markdown available.")
            if result
            else "No Markdown available."
        )
        header = ft.Column(
            [
                ft.Text(
                    result.get("metadata", {}).get("title", "No Title"),
                    weight="bold",
                    size=24,
                )
                if result
                else ft.Text("No Title", weight="bold", size=24),
                ft.Text(
                    result.get("metadata", {}).get("sourceURL", job["url"]),
                    color=ft.Colors.BLUE,
                    size=14,
                )
                if result
                else ft.Text(job["url"], size=14),
                ft.Text(f"Job ID: {job['id']}  |  Submitted: {job['timestamp']}", size=12, color=ft.Colors.GREY),
            ],
            spacing=5,
        )
        detail_tabs = ft.Tabs(
            tabs=[
                ft.Tab(
                    text="Markdown",
                    content=ft.Container(
                        content=ft.Markdown(markdown_text, expand=True),
                        padding=10,
                    ),
                ),
                ft.Tab(
                    text="JSON",
                    content=ft.Container(
                        content=ft.Text(json_response, font_family="monospace", expand=True),
                        padding=10,
                        bgcolor=ft.Colors.BLACK12,
                    ),
                ),
            ],
            selected_index=0,
            expand=True,
        )
        detail_view = ft.Column([header, ft.Divider(), detail_tabs], expand=True, spacing=10)
        self.job_detail.content = detail_view
        self.page.update()

    # ─── DB CONFIG TAB ───────────────────────────────────────────────
    def _create_db_config_tab(self):
        # SQLite UI Elements.
        self.sqlite_path_field = ft.TextField(label="SQLite DB Path", width=400)
        self.sqlite_load_tables_button = ft.ElevatedButton(text="Load Tables", on_click=self._load_sqlite_tables)
        self.sqlite_table_dropdown = ft.Dropdown(label="Select Table", width=400)
        self.sqlite_column_dropdown = ft.Dropdown(label="Select URL Column", width=400)
        self.sqlite_output_field = ft.TextField(label="Output Table Name", value="scrape_output", width=400)
        self.sqlite_batch_field = ft.TextField(label="Batch Size", value="100", width=100)
        self.sqlite_poll_field = ft.TextField(label="Poll Interval (sec)", value="5", width=100)
        self.sqlite_add_urls_button = ft.ElevatedButton(text="Add URLs", on_click=self._add_urls_sqlite)
        self.sqlite_start_polling_button = ft.ElevatedButton(text="Start Polling", on_click=self._start_polling_sqlite)
        sqlite_column = ft.Column(
            [
                self.sqlite_path_field,
                self.sqlite_load_tables_button,
                self.sqlite_table_dropdown,
                self.sqlite_column_dropdown,
                self.sqlite_output_field,
                ft.Row([self.sqlite_batch_field, self.sqlite_poll_field], spacing=10),
                ft.Row([self.sqlite_add_urls_button, self.sqlite_start_polling_button], spacing=10),
            ],
            scroll=True,
            spacing=10,
        )
        sqlite_tab = ft.Container(content=sqlite_column, padding=10)

        # PostgreSQL UI Elements.
        self.pg_host_field = ft.TextField(label="Host", value="localhost", width=200)
        self.pg_port_field = ft.TextField(label="Port", value="5432", width=100)
        self.pg_db_field = ft.TextField(label="Database", width=200)
        self.pg_user_field = ft.TextField(label="User", width=200)
        self.pg_password_field = ft.TextField(label="Password", width=200, password=True)
        self.pg_connect_button = ft.ElevatedButton(text="Connect", on_click=self._connect_pg)
        self.pg_table_dropdown = ft.Dropdown(label="Select Table", width=400)
        self.pg_column_dropdown = ft.Dropdown(label="Select URL Column", width=400)
        self.pg_output_field = ft.TextField(label="Output Table Name", value="scrape_output", width=400)
        self.pg_batch_field = ft.TextField(label="Batch Size", value="100", width=100)
        self.pg_poll_field = ft.TextField(label="Poll Interval (sec)", value="5", width=100)
        self.pg_add_urls_button = ft.ElevatedButton(text="Add URLs", on_click=self._add_urls_pg)
        self.pg_start_polling_button = ft.ElevatedButton(text="Start Polling", on_click=self._start_polling_pg)
        pg_column = ft.Column(
            [
                ft.Row([self.pg_host_field, self.pg_port_field], spacing=10),
                self.pg_db_field,
                ft.Row([self.pg_user_field, self.pg_password_field], spacing=10),
                self.pg_connect_button,
                self.pg_table_dropdown,
                self.pg_column_dropdown,
                self.pg_output_field,
                ft.Row([self.pg_batch_field, self.pg_poll_field], spacing=10),
                ft.Row([self.pg_add_urls_button, self.pg_start_polling_button], spacing=10),
            ],
            scroll=True,
            spacing=10,
        )
        pg_tab = ft.Container(content=pg_column, padding=10)

        self.db_config_tab = ft.Tabs(
            tabs=[
                ft.Tab(text="SQLite", content=sqlite_tab),
                ft.Tab(text="PostgreSQL", content=pg_tab),
            ],
            expand=True,
        )

        self.sqlite_table_dropdown.on_change = self._on_sqlite_table_changed
        self.pg_table_dropdown.on_change = self._on_pg_table_changed

    def _load_sqlite_tables(self, e):
        try:
            self.sqlite_conn = db_config.connect_sqlite(self.sqlite_path_field.value)
            tables = db_config.get_tables_sqlite(self.sqlite_conn)
            self.sqlite_table_dropdown.options = [ft.dropdown.Option(t) for t in tables]
            if tables:
                self.sqlite_table_dropdown.value = tables[0]
                self._on_sqlite_table_changed(None)
            self._show_snack("SQLite tables loaded.")
        except Exception as ex:
            self._show_snack(f"Error connecting to SQLite: {ex}")
        self.page.update()

    def _on_sqlite_table_changed(self, e):
        if self.sqlite_conn and self.sqlite_table_dropdown.value:
            columns = db_config.get_columns_sqlite(self.sqlite_conn, self.sqlite_table_dropdown.value)
            self.sqlite_column_dropdown.options = [ft.dropdown.Option(c) for c in columns]
            if columns:
                self.sqlite_column_dropdown.value = columns[0]
            self.page.update()

    def _add_urls_sqlite(self, e):
        if not self.sqlite_conn:
            self._show_snack("SQLite not connected")
            self.page.update()
            return
        table = self.sqlite_table_dropdown.value
        column = self.sqlite_column_dropdown.value
        try:
            batch_size = int(self.sqlite_batch_field.value)
        except:
            batch_size = 100
        rows = db_config.fetch_urls_sqlite(self.sqlite_conn, table, column, batch_size, self.sqlite_last_id)
        if rows:
            self.sqlite_last_id = max(row[0] for row in rows)
            for row in rows:
                url = row[1]
                new_job = self._add_job(url, {"type": "sqlite", "output_table": self.sqlite_output_field.value})
                threading.Thread(target=self._run_scraper, args=(new_job,), daemon=True).start()
            self._show_snack(f"Added {len(rows)} URLs from SQLite.")
        self.page.update()

    def _start_polling_sqlite(self, e):
        try:
            poll_interval = int(self.sqlite_poll_field.value)
        except:
            poll_interval = 5

        def poll():
            while True:
                self._add_urls_sqlite(None)
                time.sleep(poll_interval)

        threading.Thread(target=poll, daemon=True).start()
        self._show_snack("Started SQLite polling.")
        self.page.update()

    def _connect_pg(self, e):
        try:
            self.pg_conn = db_config.connect_postgres(
                self.pg_host_field.value,
                self.pg_port_field.value,
                self.pg_db_field.value,
                self.pg_user_field.value,
                self.pg_password_field.value,
            )
            tables = db_config.get_tables_postgres(self.pg_conn)
            self.pg_table_dropdown.options = [ft.dropdown.Option(t) for t in tables]
            if tables:
                self.pg_table_dropdown.value = tables[0]
                self._on_pg_table_changed(None)
            self._show_snack("Connected to PostgreSQL.")
        except Exception as ex:
            self._show_snack(f"Error connecting to PostgreSQL: {ex}")
        self.page.update()

    def _on_pg_table_changed(self, e):
        if self.pg_conn and self.pg_table_dropdown.value:
            columns = db_config.get_columns_postgres(self.pg_conn, self.pg_table_dropdown.value)
            self.pg_column_dropdown.options = [ft.dropdown.Option(c) for c in columns]
            if columns:
                self.pg_column_dropdown.value = columns[0]
            self.page.update()

    def _add_urls_pg(self, e):
        if not self.pg_conn:
            self._show_snack("PostgreSQL not connected")
            self.page.update()
            return
        table = self.pg_table_dropdown.value
        column = self.pg_column_dropdown.value
        try:
            batch_size = int(self.pg_batch_field.value)
        except:
            batch_size = 100
        rows = db_config.fetch_urls_postgres(self.pg_conn, table, column, batch_size, self.pg_last_id)
        if rows:
            self.pg_last_id = max(row[0] for row in rows)
            for row in rows:
                url = row[1]
                new_job = self._add_job(url, {"type": "postgres", "output_table": self.pg_output_field.value})
                threading.Thread(target=self._run_scraper, args=(new_job,), daemon=True).start()
            self._show_snack(f"Added {len(rows)} URLs from PostgreSQL.")
        self.page.update()

    def _start_polling_pg(self, e):
        try:
            poll_interval = int(self.pg_poll_field.value)
        except:
            poll_interval = 5

        def poll():
            while True:
                self._add_urls_pg(None)
                time.sleep(poll_interval)

        threading.Thread(target=poll, daemon=True).start()
        self._show_snack("Started PostgreSQL polling.")
        self.page.update()

    # ─── SETTINGS TAB ───────────────────────────────────────────────
    def _create_settings_tab(self):
        self.theme_toggle = ft.Switch(label="Dark Theme", value=True)
        self.theme_toggle.on_change = self._on_theme_toggle
        self.clear_jobs_button = ft.ElevatedButton(
            text="Clear Job List", icon=ft.Icons.CLEAR_ALL, on_click=self._clear_jobs
        )
        settings_column = ft.Column(
            [
                ft.Text("Settings", size=24, weight="bold"),
                ft.Row([self.theme_toggle, self.clear_jobs_button], spacing=20),
                ft.Text("Customize the application settings here."),
            ],
            alignment="start",
            spacing=20,
        )
        self.settings_tab = ft.Container(content=settings_column, padding=20)

    def _on_theme_toggle(self, e):
        if self.theme_toggle.value:
            self.page.theme = self.dark_theme
            self.page.theme_mode = ft.ThemeMode.DARK
        else:
            self.page.theme = self.light_theme
            self.page.theme_mode = ft.ThemeMode.LIGHT
        # Update widgets with theme-dependent colors.
        self.scrape_tab.bgcolor = self.get_scrape_bg()
        self.page.update()

    def _clear_jobs(self, e):
        self.jobs.clear()
        self.job_list.controls.clear()
        self.job_detail.content = ft.Text("Job list cleared.", weight="bold")
        self._show_snack("Job list cleared.")
        self.page.update()

    def _show_snack(self, message: str):
        self.page.snack_bar = ft.SnackBar(ft.Text(message))
        self.page.snack_bar.open = True
        self.page.update()

    async def _periodic_update(self):
        while True:
            self._update_job_list()
            self.page.update()
            await asyncio.sleep(1)


# Main is defined as an async function so that we can schedule the periodic update task.
async def main(page: ft.Page):
    page.theme_mode = ft.ThemeMode.DARK
    page.bgcolor = ft.Colors.BLACK
    app = ScraperApp(page)
    asyncio.create_task(app._periodic_update())


ft.app(target=main)
