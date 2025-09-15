import os
import hashlib
import sqlite3
from datetime import datetime, timedelta
import logging

class FolderDeduplicationChecker:
    def __init__(self, db_path='file_dedup.db', log_dir='logs'):
        self.db_path = db_path
        self._init_db()

        # Ensure log directory exists
        os.makedirs(log_dir, exist_ok=True)
        # Create a timestamped log file for each run
        log_filename = f"dedup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        self.log_path = os.path.join(log_dir, log_filename)

        logging.basicConfig(
            filename=self.log_path,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        logging.info("Folder Deduplication Checker initialized.")

    def _init_db(self):
        """Initialize SQLite database and table for file metadata."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id TEXT,
                filename TEXT,
                filesize INTEGER,
                checksum TEXT,
                sequence_number INTEGER,
                processed_at TEXT
            )
        ''')
        conn.commit()
        conn.close()

    def _cleanup_old_records(self, retention_days):
        """Remove old records beyond retention period."""
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM processed_files WHERE processed_at < ?', (cutoff_date.isoformat(),))
        conn.commit()
        conn.close()

    def _compute_md5(self, file_path):
        """Compute MD5 checksum for a file."""
        hash_md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def check_file(self, file_path, source_id, sequence_number=None, **node_params):
        """Check a single file for duplicates and sequence issues."""
        config = {
            "DuplicateChecking": True,
            "UseFileSize": True,
            "UseFileChecksum": True,
            "UseOriginalFilename": True,
            "SequenceChecking": True,
            "RetentionDays": 30,
            "ManualMode": False
        }
        config.update(node_params)

        filename = os.path.basename(file_path)
        try:
            logging.info(f"Processing file: {file_path}")

            if not config["DuplicateChecking"]:
                logging.info(f"[SKIP] Duplicate checking disabled for file: {file_path}")
                return True

            if config["ManualMode"]:
                logging.info(f"[SKIP] Manual mode enabled. Skipping checks for file: {file_path}")
                return True

            filesize = os.path.getsize(file_path)
            checksum = self._compute_md5(file_path)
            self._cleanup_old_records(config["RetentionDays"])

            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Filename duplicate check
            if config["UseOriginalFilename"]:
                cursor.execute('SELECT * FROM processed_files WHERE source_id=? AND filename=?',
                               (source_id, filename))
                if cursor.fetchone():
                    logging.warning(f"[REJECTED] Duplicate filename: {file_path}")
                    conn.close()
                    return False

            # File size duplicate check
            if config["UseFileSize"]:
                cursor.execute('SELECT * FROM processed_files WHERE source_id=? AND filesize=?',
                               (source_id, filesize))
                if cursor.fetchone():
                    logging.warning(f"[REJECTED] Duplicate file size: {file_path}")
                    conn.close()
                    return False

            # Checksum duplicate check
            if config["UseFileChecksum"]:
                cursor.execute('SELECT * FROM processed_files WHERE source_id=? AND checksum=?',
                               (source_id, checksum))
                if cursor.fetchone():
                    logging.warning(f"[REJECTED] Duplicate checksum: {file_path}")
                    conn.close()
                    return False

            # Sequence number validation
            if config["SequenceChecking"] and sequence_number is not None:
                cursor.execute('SELECT MAX(sequence_number) FROM processed_files WHERE source_id=?', (source_id,))
                last_seq = cursor.fetchone()[0]
                if last_seq is not None and sequence_number != last_seq + 1:
                    logging.warning(f"[SEQUENCE WARNING] {file_path}. Expected {last_seq + 1}, got {sequence_number}")

            # Insert processed file metadata
            cursor.execute(
                'INSERT INTO processed_files (source_id, filename, filesize, checksum, sequence_number, processed_at) '
                'VALUES (?, ?, ?, ?, ?, ?)',
                (source_id, filename, filesize, checksum, sequence_number, datetime.now().isoformat())
            )
            conn.commit()
            conn.close()

            logging.info(f"[ACCEPTED] File passed all checks: {file_path}")
            return True

        except Exception as e:
            logging.error(f"[ERROR] File: {file_path}, Error: {str(e)}")
            return False

    def process_folder_recursive(self, folder_path, source_id, starting_sequence=1, **node_params):
        """Recursively process all files in folder and subfolders."""
        sequence_number = starting_sequence
        results = []

        logging.info(f"Starting folder processing: {folder_path}, Source ID: {source_id}")

        for root, dirs, files in os.walk(folder_path):
            files = sorted(files)
            for file in files:
                file_path = os.path.join(root, file)
                result = self.check_file(file_path, source_id, sequence_number, **node_params)
                results.append((file_path, result))
                sequence_number += 1

        logging.info(f"Finished folder processing: {folder_path}")
        return results


# ================== Example Usage ==================
if __name__ == "__main__":
    checker = FolderDeduplicationChecker(db_path="file_dedup.db", log_dir="logs")

    # Node parameters can be dynamically set
    node_params = {
        "DuplicateChecking": True,
        "UseFileSize": False,
        "UseFileChecksum": False,
        "UseOriginalFilename": True,
        "SequenceChecking": False,
        "RetentionDays": 15,
        "ManualMode": False
    }

    folder_results = checker.process_folder_recursive(
        folder_path="in",
        source_id="NE_01",
        starting_sequence=1,
        **node_params
    )

    # Print summary
    for file_name, status in folder_results:
        print(f"{file_name} -> {'Accepted' if status else 'Rejected'}")

    print(f"\nDetailed log generated at: {checker.log_path}")
