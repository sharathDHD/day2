# file_path/formatter.py
import pandas as pd
import os

def parse_file(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    urls = []
    try:
        if ext == ".csv":
            df = pd.read_csv(file_path)
        elif ext in [".xls", ".xlsx"]:
            df = pd.read_excel(file_path)
        elif ext == ".txt":
            with open(file_path, "r", encoding="utf-8") as f:
                urls = [line.strip() for line in f if line.strip()]
            return urls
        else:
            return []
        # Assumes URLs are in the first column
        urls = df.iloc[:, 0].dropna().astype(str).tolist()
    except Exception as e:
        print(f"Error parsing file: {e}")
    return urls
