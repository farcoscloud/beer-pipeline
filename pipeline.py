# pipeline.py
# Scarica un database SQLite pubblico da Google Drive e esporta TUTTE le tabelle in CSV (separatore ';')
# Salva i CSV in ./output/ (con manifest.json). Pulisce l'output a ogni run se CLEAN_OUTPUT=1.

import os, glob, json, hashlib, sqlite3, sys, traceback
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import pandas as pd

# =========== CONFIG ===========
SRC_FOLDER_ID     = os.getenv("SRC_FOLDER_ID", "1vsVUoDGDGeVItdzkmWGZf5l0rA4gjfN4")  # cartella NUOVA
SRC_FILE_ID       = os.getenv("SRC_FILE_ID",   "")  # opzionale: ID file diretto (es. 1D0eURKnVOJAlTvHGjUDbMqcUN4_CRr2F)
TARGET_FILENAME   = os.getenv("TARGET_FILENAME", "data_raw.sqlite3")

CSV_SEPARATOR     = os.getenv("CSV_SEPARATOR", ";")
SQLITE_CHUNKSIZE  = int(os.getenv("SQLITE_CHUNKSIZE", "250000"))
CLEAN_OUTPUT      = os.getenv("CLEAN_OUTPUT", "1") == "1"

# override orario (2 modalità): run manuale Actions o variabile esplicita
GITHUB_EVENT_NAME = os.getenv("GITHUB_EVENT_NAME", "")
FORCE_RUN         = os.getenv("FORCE_RUN", "0") == "1"

WORKDIR     = os.getenv("WORKDIR", ".")
DOWNLOADDIR = os.path.join(WORKDIR, "tmp_download")
OUTPUTDIR   = os.path.join(WORKDIR, "output")
os.makedirs(DOWNLOADDIR, exist_ok=True)
os.makedirs(OUTPUTDIR,   exist_ok=True)
# =================================

def log(msg): print(f"[pipeline] {msg}", flush=True)

def md5sum(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""): h.update(chunk)
    return h.hexdigest()

def safe_name(s: str) -> str:
    return "".join(c if c.isalnum() or c in "-._" else "_" for c in s)

def clean_output_dir(out_dir: str):
    removed = 0
    for p in glob.glob(os.path.join(out_dir, "*")):
        try: os.remove(p); removed += 1
        except Exception: pass
    if removed: log(f"Output pulito: rimossi {removed} file.")

def _within_window_europe_rome(now=None):
    if now is None: now = datetime.now(ZoneInfo("Europe/Rome"))
    h = now.hour
    return (15 <= h <= 23) or (0 <= h <= 2)

def download_sqlite_from_drive() -> str:
    import gdown
    log(f"gdown version: {getattr(gdown, '__version__', 'unknown')}")
    log(f"Parametri: SRC_FILE_ID='{SRC_FILE_ID}', SRC_FOLDER_ID='{SRC_FOLDER_ID}', TARGET_FILENAME='{TARGET_FILENAME}'")

    # 1) Prova con ID file (se presente)
    if SRC_FILE_ID:
        out = os.path.join(DOWNLOADDIR, "source_by_id.sqlite3")
        log(f"Tento download diretto da file id: {SRC_FILE_ID}")
        gdown.download(id=SRC_FILE_ID, output=out, quiet=False, fuzzy=True)
        if os.path.exists(out) and os.path.getsize(out) > 0:
            log(f"OK: scaricato per ID → {out} ({os.path.getsize(out)} bytes)")
            return out
        else:
            log("ATTENZIONE: download per ID fallito o vuoto. Fallback su cartella+nome.")

    # 2) Fallback: scarica la CARTELLA e cerca per NOME
    log(f"Scarico la cartella id={SRC_FOLDER_ID} (può richiedere qualche secondo)...")
    gdown.download_folder(id=SRC_FOLDER_ID, output=DOWNLOADDIR, quiet=False, use_cookies=False)
    candidates = [p for p in glob.glob(os.path.join(DOWNLOADDIR, "**", "*"), recursive=True)
                  if os.path.basename(p) == TARGET_FILENAME and os.path.isfile(p)]
    log(f"Candidati trovati per nome '{TARGET_FILENAME}': {len(candidates)}")
    for c in candidates[:5]:
        log(f"  - {c} ({os.path.getsize(c)} bytes)")
    if not candidates:
        raise FileNotFoundError(f"'{TARGET_FILENAME}' non trovato nella cartella sorgente.")

    # se più candidati, scegli il più recente per mtime
    best = max(candidates, key=lambda p: os.path.getmtime(p))
    log(f"Selezionato: {best}")
    return best

def export_all_tables_sqlite(db_path: str, out_dir: str, sep: str = ";", chunksize: int = 200_000):
    exported = []
    con = sqlite3.connect(db_path)
    try:
        tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name", con)["name"].tolist()
        if not tables: raise ValueError("Nessuna tabella trovata nel database.")
        log(f"Trovate {len(tables)} tabelle. Esporto in CSV con chunksize={chunksize}...")

        for t in tables:
            out_csv = os.path.join(out_dir, f"{safe_name(t)}.csv")
            first, rows_total = True, 0
            try:
                for chunk in pd.read_sql_query(f"SELECT * FROM '{t}'", con, chunksize=chunksize):
                    rows_total += len(chunk)
                    chunk.to_csv(out_csv, index=False, sep=sep, mode=("w" if first else "a"), header=first)
                    first = False
                info = {"table": t, "csv_path": out_csv, "rows": rows_total, "md5": md5sum(out_csv)}
                exported.append(info)
                log(f"✓ {t} -> {out_csv} (rows={rows_total}, md5={info['md5']})")
            except Exception as e:
                log(f"⚠️ ERRORE su tabella {t}: {e}")
    finally:
        con.close()
    return exported

def write_manifest(out_dir: str, files_info: list):
    manifest = {
        "run_date_iso": datetime.now(timezone.utc).isoformat(),
        "source": {"folder_id": SRC_FOLDER_ID, "file_id": SRC_FILE_ID, "target_filename": TARGET_FILENAME},
        "csv_separator": CSV_SEPARATOR,
        "files": [{"table": fi["table"], "filename": os.path.basename(fi["csv_path"]), "rows": fi["rows"], "md5": fi["md5"]} for fi in files_info]
    }
    mpath = os.path.join(out_dir, "manifest.json")
    with open(mpath, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    log(f"Manifest scritto: {mpath}")
    return mpath

def main():
    # Log iniziali
    now_it = datetime.now(ZoneInfo("Europe/Rome")).strftime("%Y-%m-%d %H:%M:%S %Z")
    log(f"Start run — Europe/Rome now = {now_it}")
    log(f"WORKDIR={os.path.abspath(WORKDIR)}  OUTPUTDIR={os.path.abspath(OUTPUTDIR)}  DOWNLOADDIR={os.path.abspath(DOWNLOADDIR)}")
    log(f"GITHUB_EVENT_NAME={GITHUB_EVENT_NAME!r}  FORCE_RUN={FORCE_RUN}")

    # Guard orario con override
    if not FORCE_RUN and GITHUB_EVENT_NAME != "workflow_dispatch":
        if not _within_window_europe_rome():
            log("Fuori finestra oraria (Europe/Rome 15:00–02:00). Skip. (Lancia manualmente o imposta FORCE_RUN=1 per forzare)")
            return
        else:
            log("Dentro la finestra oraria: procedo.")
    else:
        log("Override orario attivo (run manuale o FORCE_RUN=1): procedo anche fuori finestra.")

    # Pulizia output
    if CLEAN_OUTPUT:
        clean_output_dir(OUTPUTDIR)

    # Download DB
    db_path = download_sqlite_from_drive()
    size = os.path.getsize(db_path)
    log(f"DB locale: {db_path}  size: {size} bytes")

    # Export tabelle
    files_info = export_all_tables_sqlite(db_path, OUTPUTDIR, sep=CSV_SEPARATOR, chunksize=SQLITE_CHUNKSIZE)

    # Manifest + elenco file finali
    write_manifest(OUTPUTDIR, files_info)
    log("File generati in output/:")
    for p in sorted(glob.glob(os.path.join(OUTPUTDIR, "*"))):
        log(f"  - {os.path.basename(p)}  ({os.path.getsize(p)} bytes)")

if __name__ == "__main__":
    try:
        main()
        log("Run COMPLETATO.")
    except Exception as e:
        log("ERRORE FATALE:")
        traceback.print_exc()
        sys.exit(1)
