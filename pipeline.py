ì# pipeline.py
# Scarica un database SQLite pubblico da Google Drive e esporta TUTTE le tabelle in CSV (separatore ';')
# Salva i CSV in ./output/ (con manifest.json riassuntivo). Pulisce l'output a ogni run se CLEAN_OUTPUT=1.

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
    if now is None:
        now = datetime.now(ZoneInfo("Europe/Rome"))
    h = now.hour
    return (15 <= h <= 23) or (0 <= h <= 2)

if not _within_window_europe_rome():
    print("[pipeline] Fuori finestra oraria (Europe/Rome 15:00–02:00). Skip.")
    sys.exit(0)
# --- fine guard ---

# =========== CONFIG ===========
SRC_FOLDER_ID   = os.getenv("SRC_FOLDER_ID", "1vsVUoDGDGeVItdzkmWGZf5l0rA4gjfN4")  # NUOVA cartella Drive
SRC_FILE_ID     = os.getenv("SRC_FILE_ID",   "")  # opzionale: se vuoi forzare l'ID file diretto
TARGET_FILENAME = os.getenv("TARGET_FILENAME", "data_raw.sqlite3")

CSV_SEPARATOR    = os.getenv("CSV_SEPARATOR", ";")
SQLITE_CHUNKSIZE = int(os.getenv("SQLITE_CHUNKSIZE", "250000"))
CLEAN_OUTPUT     = os.getenv("CLEAN_OUTPUT", "1") == "1"

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

def clean_output_dir(out_dir: str):
    removed = 0
    for p in glob.glob(os.path.join(out_dir, "*")):
        try:
            os.remove(p)
            removed += 1
        except Exception:
            pass
    if removed:
        print(f"[pipeline] Output pulito: rimossi {removed} file.")

def download_sqlite_from_drive() -> str:
    """
    Usa gdown per scaricare il DB:
    - se SRC_FILE_ID è impostato -> scarica direttamente
    - altrimenti scarica tutta la cartella e cerca TARGET_FILENAME
    Ritorna il path locale del DB.
    """
    import gdown

    if SRC_FILE_ID:
        out = os.path.join(DOWNLOADDIR, "source.sqlite3")
        gdown.download(id=SRC_FILE_ID, output=out, quiet=True, fuzzy=True)
        if not os.path.exists(out):
            raise FileNotFoundError("Download dal file ID fallito.")
        return out

    # fallback: cerca per nome dentro la cartella
    gdown.download_folder(id=SRC_FOLDER_ID, output=DOWNLOADDIR, quiet=True, use_cookies=False)
    candidates = [p for p in glob.glob(os.path.join(DOWNLOADDIR, "**", "*"), recursive=True)
                  if os.path.basename(p) == TARGET_FILENAME and os.path.isfile(p)]
    if not candidates:
        raise FileNotFoundError(f"'{TARGET_FILENAME}' non trovato nella cartella sorgente.")
    return candidates[0]

def export_all_tables_sqlite(db_path: str, out_dir: str, sep: str = ";", chunksize: int = 200_000):
    """
    Esporta tutte le tabelle del DB in CSV (uno per tabella).
    Ritorna: lista di dict con info file.
    """
    exported = []
    con = sqlite3.connect(db_path)
    try:
        tables = pd.read_sql_query(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name", con
        )["name"].tolist()
        if not tables:
            raise ValueError("Nessuna tabella trovata nel database.")

        print(f"[pipeline] Trovate {len(tables)} tabelle. Esporto in CSV...")
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
                info = {
                    "table": t,
                    "csv_path": out_csv,
                    "rows": rows_total,
                    "md5": md5sum(out_csv)
                }
                exported.append(info)
                print(f"[pipeline] ✓ {t} -> {out_csv} (rows={rows_total})")
            except Exception as e:
                print(f"[pipeline] ⚠️ ERRORE su tabella {t}: {e}")
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
    print(f"[pipeline] Manifest scritto: {mpath}")
    return mpath

def main():
    if CLEAN_OUTPUT:
        clean_output_dir(OUTPUTDIR)

    db_path = download_sqlite_from_drive()
    size = os.path.getsize(db_path)
    print(f"[pipeline] DB locale: {db_path}  size: {size} bytes")

    files_info = export_all_tables_sqlite(db_path, OUTPUTDIR, sep=CSV_SEPARATOR, chunksize=SQLITE_CHUNKSIZE)
    write_manifest(OUTPUTDIR, files_info)

if __name__ == "__main__":
    main()
