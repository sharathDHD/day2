import tkinter as tk
from tkinter import scrolledtext
import threading
import time
import main as scraper_main  # Import main.py as a module

# List of 10 URLs to scrape. You can customize these URLs.
URLS = [
    "https://www.example.com",
    "https://www.python.org",
    "https://www.wikipedia.org",
    "https://www.github.com",
    "https://www.stackoverflow.com",
    "https://www.reddit.com",
    "https://www.medium.com",
    "https://www.quora.com",
    "https://www.linkedin.com",
    "https://www.facebook.com"
]

def update_log(log_widget: scrolledtext.ScrolledText, msg: str) -> None:
    """
    Append a message to the log widget.
    """
    log_widget.configure(state='normal')
    log_widget.insert(tk.END, msg + "\n")
    log_widget.configure(state='disabled')
    log_widget.yview(tk.END)

def scrape_all(log_widget: scrolledtext.ScrolledText) -> None:
    """
    Loop through the list of URLs, scrape each one by calling main.main(url),
    and update the log widget with the progress.
    """
    for url in URLS:
        update_log(log_widget, f"Starting scrape for: {url}")
        # Call the main function from main.py to scrape the URL and store the result.
        scraper_main.main(url)
        update_log(log_widget, f"Finished scraping: {url}")
        # Optional: sleep briefly between scrapes to avoid hammering servers.
        time.sleep(1)
    update_log(log_widget, "All scraping completed.")

def start_scraping(log_widget: scrolledtext.ScrolledText) -> None:
    """
    Start the scraping process in a separate thread.
    """
    thread = threading.Thread(target=scrape_all, args=(log_widget,))
    thread.daemon = True  # Optional: ensures thread exits when main program exits.
    thread.start()

def build_gui() -> None:
    """
    Build and run the Tkinter GUI.
    """
    root = tk.Tk()
    root.title("Scraper GUI - 10 URLs")

    # Create a button to start scraping.
    scrape_button = tk.Button(root, text="Scrape 10 URLs", 
                              command=lambda: start_scraping(log_text))
    scrape_button.pack(pady=10)

    # Create a scrolled text widget to display logs.
    global log_text
    log_text = scrolledtext.ScrolledText(root, width=80, height=20, state='disabled')
    log_text.pack(padx=10, pady=10)

    root.mainloop()

if __name__ == "__main__":
    build_gui()
