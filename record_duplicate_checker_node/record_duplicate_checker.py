import os
import json
import logging
import sqlite3
import threading
import time
import re
import signal
import sys
from pathlib import Path
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler
from typing import List
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from concurrent.futures import ThreadPoolExecutor, as_completed

# =============================
# Configuration
# =============================
CONFIG = {
    "EnableRecDuplicateCheck": True,
    "input_dir": "in",
    "accepted_dir": "accepted",
    "rejected_dir": "rejected",
    "failed_dir": "failed",
    "archive_dir": "archive",
    "db_file": "duplicate_checker.db",
    "log_file": "rdc.log",
    "retention_hours": 72,
    "cleanup_interval_seconds": 3600,
    "log_rotation_when": "midnight",
    "log_rotation_interval": 1,
    "max_workers": 5,
    "db_retry_max": 5,
    "db_retry_wait": 0.1,  # seconds
}

# =============================
# Logging Setup
# =============================
logger = logging.getLogger()
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(ch)

fh = TimedRotatingFileHandler(
    CONFIG["log_file"],
    when=CONFIG["log_rotation_when"],
    interval=CONFIG["log_rotation_interval"],
    backupCount=14,
    utc=True
)
fh.setLevel(logging.INFO)
fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(fh)

# =============================
# Globals
# =============================
_db_lock = threading.Lock()
_shutdown = threading.Event()
_cleanup_thread = None
_db_conn = None  # Single shared connection

# =============================
# Helper Functions
# =============================
def make_dirs():
    for d in [CONFIG["input_dir"], CONFIG["accepted_dir"], CONFIG["rejected_dir"],
              CONFIG["failed_dir"], CONFIG["archive_dir"]]:
        Path(d).mkdir(parents=True, exist_ok=True)

def _extract_rating_groups_from_generic(generic: dict) -> List[str]:
    rgs = []
    if not isinstance(generic, dict):
        return rgs
    for ext in generic.get("recordExtensions", []) or []:
        if ext.get("recordProperty") != "listOfMscc":
            continue
        for sub in ext.get("recordSubExtensions", []) or []:
            if sub.get("recordProperty") == "mscc":
                relems = sub.get("recordElements", {}) or {}
                rg = relems.get("ratingGroup")
                if rg is not None:
                    rgs.append(str(rg))
    return list(dict.fromkeys(rgs))

def build_composite_keys_for_record(rec_entry: dict) -> List[str]:
    gen = rec_entry.get("payload", {}).get("genericRecord", {}) or rec_entry.get("genericRecord") or {}
    elems = gen.get("recordElements", {}) or {}
    sid = elems.get("sessionId")
    seq = elems.get("sessionSequenceNumber")
    if sid is None or seq is None:
        logging.warning("Missing sessionId/sessionSequenceNumber for recordId=%s", elems.get("recordId"))
        return []
    rgs = _extract_rating_groups_from_generic(gen)
    if not rgs:
        return [f"{sid}|{seq}|__NO_RG__"]
    return [f"{sid}|{seq}|{rg}" for rg in rgs]

def _safe_filename(name: str):
    if not name:
        name = f"record_{int(time.time())}"
    name = re.sub(r'[\/:*?"<>|]', "_", name).strip()
    return name or f"record_{int(time.time())}"

def write_record(record_obj, directory, record_id):
    try:
        Path(directory).mkdir(parents=True, exist_ok=True)
        safe = _safe_filename(record_id)
        filepath = Path(directory) / f"{safe}.json"
        tmp = filepath.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(record_obj, f, indent=2, ensure_ascii=False)
        tmp.replace(filepath)
        return True
    except Exception as e:
        logging.exception("Failed to write record %s to %s: %s", record_id, directory, e)
        return False

def archive_file(input_file):
    archive_path = Path(CONFIG["archive_dir"]) / Path(input_file).name
    if archive_path.exists():
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
        archive_path = archive_path.with_name(f"{archive_path.stem}_{timestamp}{archive_path.suffix}")
    Path(input_file).rename(archive_path)
    logging.info("Input file archived: %s", archive_path)

# =============================
# Database Helpers
# =============================
def init_db(db_file):
    Path(db_file).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_file, timeout=30, isolation_level=None, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rec_dup_keys (
            composite_key TEXT PRIMARY KEY,
            ts TEXT
        )
    """)
    conn.commit()
    return conn

def cleanup_old_keys(retention_hours: int):
    try:
        cutoff = (datetime.utcnow() - timedelta(hours=retention_hours)).isoformat()
        with _db_lock:
            cur = _db_conn.cursor()
            cur.execute("BEGIN")
            cur.execute("DELETE FROM rec_dup_keys WHERE ts < ?", (cutoff,))
            deleted = cur.rowcount
            _db_conn.commit()
        logging.info("Cleanup: purged %s old key rows (ts < %s)", deleted, cutoff)
    except Exception:
        try:
            _db_conn.rollback()
        except Exception:
            pass
        logging.exception("Error during cleanup_old_keys")

def _cleanup_worker():
    interval = CONFIG.get("cleanup_interval_seconds", 3600)
    retention = CONFIG.get("retention_hours", 72)
    logging.info("Starting cleanup worker: interval=%s seconds, retention=%s hours", interval, retention)
    while not _shutdown.wait(interval):
        cleanup_old_keys(retention)
    logging.info("Cleanup worker stopped")

# =============================
# Core Duplicate Check Logic
# =============================
def process_record(record, input_file):
    gen = record.get("payload", {}).get("genericRecord", {}) or record.get("genericRecord", {}) or {}
    elems = gen.get("recordElements", {}) or {}
    record_id = elems.get("recordId") or f"noid_{int(datetime.now().timestamp())}"
    composite_keys = build_composite_keys_for_record(record)

    if not composite_keys:
        write_record(record, CONFIG["failed_dir"], record_id)
        logging.error("Record moved to failed: missing composite key fields recordId=%s (file=%s)", record_id, input_file)
        return "failed"

    if CONFIG["EnableRecDuplicateCheck"]:
        success = False
        for attempt in range(CONFIG["db_retry_max"]):
            try:
                with _db_lock:
                    cur = _db_conn.cursor()
                    now_iso = datetime.utcnow().isoformat()
                    for key in composite_keys:
                        cur.execute("INSERT INTO rec_dup_keys (composite_key, ts) VALUES (?, ?)", (key, now_iso))
                    _db_conn.commit()
                success = True
                break
            except sqlite3.IntegrityError:
                _db_conn.rollback()
                write_record(record, CONFIG["rejected_dir"], record_id)
                logging.warning("Record rejected (duplicate): recordId=%s keys=%s (file=%s)", record_id, composite_keys, input_file)
                return "duplicate"
            except sqlite3.OperationalError as e:
                _db_conn.rollback()
                if "locked" in str(e).lower():
                    logging.warning("Database locked, retrying... attempt %d", attempt + 1)
                    time.sleep(CONFIG["db_retry_wait"])
                else:
                    break
        if not success:
            write_record(record, CONFIG["failed_dir"], record_id)
            logging.error("Failed to insert record after retries: recordId=%s", record_id)
            return "failed"

        write_record(record, CONFIG["accepted_dir"], record_id)
        logging.info("Record accepted: recordId=%s keys=%s (file=%s)", record_id, composite_keys, input_file)
        return "accepted"
    else:
        write_record(record, CONFIG["accepted_dir"], record_id)
        logging.info("Duplicate check disabled. Record accepted: recordId=%s (file=%s)", record_id, input_file)
        return "accepted"

# =============================
# File Processing
# =============================
def process_file(input_file):
    logging.info("Processing file: %s", input_file)
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            loaded = json.load(f)
    except Exception:
        logging.exception("Failed to read input file %s", input_file)
        Path(input_file).rename(Path(CONFIG["failed_dir"]) / Path(input_file).name)
        return

    records_to_process = []
    if isinstance(loaded, dict) and "records" in loaded and isinstance(loaded["records"], dict):
        records_to_process = list(loaded["records"].values())
    elif isinstance(loaded, list):
        records_to_process = loaded
    elif isinstance(loaded, dict):
        records_to_process = [loaded]
    else:
        logging.error("Unrecognized JSON structure in %s", input_file)
        Path(input_file).rename(Path(CONFIG["failed_dir"]) / Path(input_file).name)
        return

    accepted = rejected = failed = 0
    with ThreadPoolExecutor(max_workers=CONFIG["max_workers"]) as executor:
        futures = {executor.submit(process_record, rec, input_file): rec for rec in records_to_process}
        for future in as_completed(futures):
            result = future.result()
            if result == "accepted":
                accepted += 1
            elif result == "duplicate":
                rejected += 1
            else:
                failed += 1

    logging.info("SUMMARY: file=%s, total=%s, accepted=%s, duplicate=%s, failed=%s",
                 input_file, len(records_to_process), accepted, rejected, failed)

    archive_file(input_file)

def process_existing_files(folder_path):
    folder = Path(folder_path)
    for filepath in folder.glob("*.json"):
        logging.info("Processing existing file: %s", filepath)
        process_file(filepath)

# =============================
# Watchdog Folder Watcher
# =============================
class RecordFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        filepath = event.src_path
        if filepath.lower().endswith(".json"):
            logging.info("New file detected: %s", filepath)
            process_file(filepath)

def watch_input_folder(folder_path):
    Path(folder_path).mkdir(parents=True, exist_ok=True)
    event_handler = RecordFileHandler()
    observer = Observer()
    observer.schedule(event_handler, folder_path, recursive=True)
    observer.start()
    logging.info("Watching folder recursively for new JSON files: %s", folder_path)
    try:
        while not _shutdown.is_set():
            time.sleep(1)
    finally:
        logging.info("Stopping folder watch...")
        observer.stop()
        observer.join()

# =============================
# Signal handling
# =============================
def _signal_handler(signum, frame):
    logging.info("Signal %s received: shutting down", signum)
    _shutdown.set()
    if _cleanup_thread:
        _cleanup_thread.join()
    sys.exit(0)

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# =============================
# Main
# =============================
if __name__ == "__main__":
    make_dirs()
    _db_conn = init_db(CONFIG["db_file"])

    _cleanup_thread = threading.Thread(target=_cleanup_worker, daemon=True)
    _cleanup_thread.start()

    # Process any existing JSON files first
    process_existing_files(CONFIG["input_dir"])

    # Then start folder watcher
    watch_input_folder(CONFIG["input_dir"])
