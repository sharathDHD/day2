# file_path/main.py
import flet as ft
import threading
import json
from scraper import run_job
from formatter import parse_file

# Global job list: each job is a dict with keys: id, url, status, response (a dict), etc.
jobs = []

def add_job(url):
    job = {
        "id": len(jobs) + 1,
        "url": url,
        "status": "in queue",
        "response": {},
    }
    jobs.append(job)
    return job

def run_scraper(job, update_job_callback):
    job["status"] = "in progress"
    update_job_callback()
    result = run_job(job["url"])
    # Save the full JSON response from run_job
    job["response"] = result
    job["status"] = "completed" if result.get("metadata", {}).get("statusCode") == 200 else "error"
    update_job_callback()

def main(page: ft.Page):
    page.title = "Flet Scraper App"

    # ----------------------------
    # Scrape Tab Components
    # ----------------------------
    url_field = ft.TextField(label="Enter URL", width=400)
    def on_add_job(e):
        url = url_field.value
        if url:
            job = add_job(url)
            update_job_list()
            threading.Thread(target=run_scraper, args=(job, update_job_list), daemon=True).start()
            url_field.value = ""
            page.update()

    add_job_button = ft.ElevatedButton(text="Add Job", on_click=on_add_job)
    file_picker = ft.FilePicker(on_result=lambda e: on_file_upload_result(e))
    page.overlay.append(file_picker)
    file_upload_button = ft.ElevatedButton(text="Upload File", on_click=lambda e: file_picker.pick_files())
    scrape_tab_content = ft.Column(
        [
            ft.Row([url_field, add_job_button, file_upload_button]),
            ft.Text("Use this tab to add new scraping jobs.")
        ],
        scroll=True
    )

    # ----------------------------
    # Jobs List Tab Components
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
        page.update()

    def show_job_detail(job):
        # Prepare the JSON response text (pretty printed)
        result = job["response"]
        json_response = json.dumps(result, indent=2) if result else "{}"
        # If available, use the markdown text from the JSON response.
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
        page.update()

    def on_file_upload_result(e: ft.FilePickerResultEvent):
        if e.files:
            urls = parse_file(e.files[0].path)
            for url in urls:
                add_job(url)
            update_job_list()

    # ----------------------------
    # Tabs Setup
    # ----------------------------
    tabs = ft.Tabs(
        tabs=[
            ft.Tab(text="Scrape", content=scrape_tab_content),
            ft.Tab(
                text="Jobs List",
                content=ft.Row(
                    [
                        job_list,
                        ft.VerticalDivider(),
                        job_detail,
                    ],
                    expand=True
                ),
            ),
        ],
        expand=True
    )

    page.add(tabs)

ft.app(target=main)
