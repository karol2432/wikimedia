import bz2
import os
import re
from pathlib import Path
import requests
from pymongo import UpdateOne
from tqdm import tqdm
from src.config import get_local_db


DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

BASE_URL = "https://dumps.wikimedia.org/other/pageview_complete/2026/2026-02"


TARGET_DATES = [
    "20260213",  
    "20260214",  
    "20260215", 
]

ALLOWED_PROJECTS = {"pl.wikipedia", "en.wikipedia"}


EXCLUDED_PREFIXES = (
    "Specjalna:", "Special:", "Plik:", "File:", "Wikipedia:", "Wikipedysta:",
    "User:", "Kategoria:", "Category:", "Szablon:", "Template:", "Pomoc:",
    "Help:", "Portal:", "Dyskusja:", "Talk:", "MediaWiki:", "Module:", "Moduł:"
)
EXCLUDED_EXACT = {"-", "Main_Page", "Wikipedia:Strona_główna"}


MIN_VIEWS = 50

#slownik do dekodowania godzin (A=0, B=1, ..., X=23)
HOUR_MAP = {chr(ord("A") + i): i for i in range(24)}


def parse_hourly_counts(hourly_str: str) -> dict:
    """dekoduje ciąg typu 'A12B5X30' na słownik {'0': 12, '1': 5, '23': 30}"""
    hourly = {}
    for letter, count in re.findall(r"([A-X])(\d+)", hourly_str):
        hour = str(HOUR_MAP[letter])
        hourly[hour] = int(count)
    return hourly


def download_dump(date_str: str) -> Path:
    """Pobiera plik .bz2 z serwerów Wikimedia, jeśli jeszcze nie istnieje lokalnie"""
    filename = f"pageviews-{date_str}-user.bz2"
    filepath = DATA_DIR / filename

    if filepath.exists() and filepath.stat().st_size > 10_000_000:
        print(f"[SKIP] Plik {filename} już istnieje lokalnie ({filepath.stat().st_size / (1024**2):.1f} MB).")
        return filepath

    url = f"{BASE_URL}/{filename}"
    print(f"[DOWNLOAD] Pobieranie {url} ...")
    headers = {"User-Agent": "StudentBigDataProject/1.0 (kontakt@example.com)"}

    with requests.get(url, stream=True, headers=headers, timeout=60) as r:
        r.raise_for_status()
        total_size = int(r.headers.get("content-length", 0))
        with open(filepath, "wb") as f, tqdm(
            total=total_size, unit="B", unit_scale=True, desc=filename
        ) as pbar:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))

    return filepath


def process_and_load_dump(filepath: Path, date_str: str, collection, batch_size: int = 5000):
    """rozpakowuje .bz2, czyści dane i zapisuje do MongoDB"""
    raw_bytes_uncompressed = 0
    total_lines = 0
    valid_records = 0
    rejected_records = 0
    operations = []

    formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    print(f"[ETL] Przetwarzanie {filepath.name} (data: {formatted_date})...")

    with bz2.open(filepath, mode="rt", encoding="utf-8", errors="replace") as f:
        for line in f:
            raw_bytes_uncompressed += len(line.encode("utf-8"))
            total_lines += 1

            parts = line.rstrip("\n").split(" ")

            if len(parts) < 5:
                rejected_records += 1
                continue

            project, article, page_id_raw, access, daily_total_raw = parts[:5]
            hourly_raw = parts[5] if len(parts) > 5 else ""

            if project not in ALLOWED_PROJECTS:
                rejected_records += 1
                continue

            if not article or article in EXCLUDED_EXACT or article.startswith(EXCLUDED_PREFIXES):
                rejected_records += 1
                continue

            try:
                daily_total = int(daily_total_raw)
            except ValueError:
                rejected_records += 1
                continue

            if daily_total < MIN_VIEWS:
                rejected_records += 1
                continue

            page_id = int(page_id_raw) if page_id_raw.isdigit() else None
            hourly_counts = parse_hourly_counts(hourly_raw)

           
            doc_id = f"{formatted_date}|{project}|{article}|{access}"

            doc = {
                "date": formatted_date,
                "project": project,
                "article": article,
                "page_id": page_id,
                "access": access,
                "daily_total": daily_total,
                "hourly_counts": hourly_counts,
            }

            operations.append(
                UpdateOne({"_id": doc_id}, {"$set": doc}, upsert=True)
            )
            valid_records += 1

            if len(operations) >= batch_size:
                collection.bulk_write(operations, ordered=False)
                operations.clear()

            if total_lines % 2_000_000 == 0:
                print(
                    f"   ...przeczytano {total_lines:,} linii "
                    f"({raw_bytes_uncompressed / (1024**3):.2f} GB po wypakowaniu) | "
                    f"zapisano: {valid_records:,}"
                )

    if operations:
        collection.bulk_write(operations, ordered=False)

    gb_uncompressed = raw_bytes_uncompressed / (1024 ** 3)
    print(
        f"[DONE] {filepath.name}: przeczytano {total_lines:,} linii "
        f"({gb_uncompressed:.2f} GB danych po wypakowaniu) | "
        f"Zapisano czystych: {valid_records:,} | Odrzucono: {rejected_records:,}"
    )
    return gb_uncompressed, valid_records


def run_etl(dates=None):
    if dates is None:
        dates = TARGET_DATES

    db = get_local_db()
    collection = db["pageviews"]

    collection.create_index([("project", 1), ("date", 1)])
    collection.create_index([("project", 1), ("daily_total", -1)])
    collection.create_index([("article", 1)])

    total_gb = 0.0
    total_saved = 0

    for d in dates:
        filepath = download_dump(d)
        gb, saved = process_and_load_dump(filepath, d, collection)
        total_gb += gb
        total_saved += saved

    print("\n=== PODSUMOWANIE ETL ===")
    print(f"Łączny rozmiar przetworzonych danych po wypakowaniu: {total_gb:.2f} GB")
    print(f"Łączna liczba przetworzonych rekordów (upsert): {total_saved:,}")
    print(f"Aktualna liczba dokumentów w kolekcji 'pageviews': {collection.count_documents({}):,}")


if __name__ == "__main__":
    run_etl()