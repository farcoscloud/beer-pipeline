# pipeline.py
# Scarica un database SQLite pubblico da Google Drive e esporta TUTTE le tabelle in CSV (separatore ';')
# Salva i CSV in ./output/ (con manifest.json riassuntivo)

import os
import glob
import json
import hashlib
import sqlite3
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd

# --- GUARD: esegui solo tra 15:00 e 02:00 (Europe/Rome) ---
def _within_window_europe_rome(now=None):
    """
    Ritorna True se l'orario corrente in Europe/Rome è tra:
    - 15:00 -> 23:59 (compresi)
    - 00:00 -> 02:00 (compresi)
    """
    if now is None:
        now = datetime.now(ZoneInfo("Europe/Rome"))
    h = now.hour
    return (15 <= h <= 23) or (0 <= h <= 2)

if not _within_window_europe_rome():
    print("[pipeline] Fuori finestra oraria (Europe/Rome 15:00–02:00). Skip.")
    sys.exit(0)
# --- fine guard ---

# =========== CONFIG ===========
SRC_FOLDER_ID   = os.getenv("SRC_FOLDER_ID", "1TAi1PJ3eCWS__1OgbbMfIZ9rmnOXK--1")
SRC_FILE_ID     = os.getenv("SRC_FILE_ID",   "1-FF8-2QtdlPruzzIJSy98Nhlc9n5LyeE")
TARGET_FILENAME = os.getenv("TARGET_FILENAME", "data_raw.sqlite3")

CSV_SEPARATOR    = os.getenv("CSV_SEPARATOR", ";")
SQLITE_CHUNKSIZE = int(os.getenv("SQLITE_CHUNKSIZE", "250000"))

WORKDIR     = os.getenv("WORKDIR", ".")
DOWNLOADDIR = os.path.join(WORKDIR, "tmp_download")
OUTPUTDIR   = os.path.join(WORKDIR, "output")
os.makedirs(DOWNLOADDIR, exist_ok=True)
os.makedirs(OUTPUTDIR, exist_ok=True)
# =================================

def md5sum(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

def safe_name(s: str) -> str:
    return "".join(c if c.isalnum() or c in "-._" else "_" for c in s)

def download_sqlite_from_drive() -> str:
    """Scarica il DB da Google Drive (file o cartella pubblica)."""
    import gdown

    if SRC_FILE_ID:
        out = os.path.join(DOWNLOADDIR, "source.sqlite3")
        gdown.download(id=SRC_FILE_ID, output=out, quiet=True, fuzzy=True)
        if not os.path.exists(out):
            raise FileNotFoundError("Download dal file ID fallito.")
        return out

    gdown.download_folder(id=SRC_FOLDER_ID, output=DOWNLOADDIR, quiet=True, use_cookies=False)
    candidates = [p for p in glob.glob(os.path.join(DOWNLOADDIR, "**", "*"), recursive=True)
                  if os.path.basename(p) == TARGET_FILENAME and os.path.isfile(p)]
    if not candidates:
        raise FileNotFoundError(f"'{TARGET_FILENAME}' non trovato nella cartella sorgente.")
    return candidates[0]

def export_all_tables_sqlite(db_path: str, out_dir: str, sep: str = ";", chunksize: int = 200_000):
    """Esporta tutte le tabelle del DB in CSV (uno per tabella)."""
    exported = []
    con = sqlite3.connect(db_path)
    try:
        tables = pd.read_sql_query(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name", con
        )["name"].tolist()
        if not tables:
            raise ValueError("Nessuna tabella trovata nel database.")

        print(f"Trovate {len(tables)} tabelle. Esporto in CSV...")
        for t in tables:
            out_csv = os.path.join(out_dir, f"{safe_name(t)}.csv")
            first = True
            rows_total = 0
            try:
                for chunk in pd.read_sql_query(f"SELECT * FROM '{t}'", con, chunksize=chunksize):
                    rows_total += len(chunk)
                    chunk.to_csv(out_csv, index=False, sep=sep,
                                 mode="w" if first else "a", header=first)
                    first = False
                exported.append({
                    "table": t,
                    "csv_path": out_csv,
                    "rows": rows_total,
                    "md5": md5sum(out_csv)
                })
                print(f"✓ {t} -> {out_csv} (rows={rows_total})")
            except Exception as e:
                print(f"⚠️ ERRORE su tabella {t}: {e}")
    finally:
        con.close()
    return exported

def write_manifest(out_dir: str, files_info: list):
    manifest = {
        "run_date_iso": datetime.now(timezone.utc).isoformat(),
        "source": {
            "folder_id": SRC_FOLDER_ID,
            "file_id": SRC_FILE_ID,
            "target_filename": TARGET_FILENAME
        },
        "csv_separator": CSV_SEPARATOR,
        "files": [
            {
                "table": fi["table"],
                "filename": os.path.basename(fi["csv_path"]),
                "rows": fi["rows"],
                "md5": fi["md5"]
            } for fi in files_info
        ]
    }
    mpath = os.path.join(out_dir, "manifest.json")
    with open(mpath, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print(f"Manifest scritto: {mpath}")
    return mpath

def main():
    db_path = download_sqlite_from_drive()
    print("DB locale:", db_path, "size:", os.path.getsize(db_path), "bytes")

    files_info = export_all_tables_sqlite(db_path, OUTPUTDIR, sep=CSV_SEPARATOR, chunksize=SQLITE_CHUNKSIZE)
    write_manifest(OUTPUTDIR, files_info)

if __name__ == "__main__":
    main()
