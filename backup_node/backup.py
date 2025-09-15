import os
import shutil
import gzip
import logging
from datetime import datetime

# ----------------------------
# Logging Configuration
# ----------------------------
log_file = "backup_delivery.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# ----------------------------
# Configuration
# ----------------------------
backup_root = "/backup/NCC/CDR/Processed"

# Example nodes (you can add more)
nodes = [
    {"name": "Billing", "source_dir": "in", "compression": True},
]

max_days = 15
max_files = 1000


# ----------------------------
# Utility Functions
# ----------------------------
def compress_file(src, dest):
    """Compress file to .gz with error handling"""
    try:
        with open(src, "rb") as f_in, gzip.open(dest, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        return True
    except Exception as e:
        logging.error(f"❌ Compression failed for {src}: {e}")
        return False


def copy_file(src, dest):
    """Copy file with error handling"""
    try:
        shutil.copy2(src, dest)
        return True
    except Exception as e:
        logging.error(f"❌ Copy failed for {src}: {e}")
        return False


def backup_node(node):
    """Backup all files for one node with error handling & counters"""
    node_name = node["name"]
    src_dir = node["source_dir"]
    compress = node.get("compression", False)

    if not os.path.exists(src_dir):
        logging.error(f"❌ Source directory not found: {src_dir}")
        return {"copied": 0, "compressed": 0, "failed": 0}

    # Backup destination with date
    date_str = datetime.now().strftime("%Y-%m-%d")
    dest_dir = os.path.join(backup_root, node_name, date_str)

    try:
        os.makedirs(dest_dir, exist_ok=True)
    except Exception as e:
        logging.critical(f"❌ Cannot create backup directory {dest_dir}: {e}")
        return {"copied": 0, "compressed": 0, "failed": 0}

    logging.info(f"📂 Starting backup for node [{node_name}] from {src_dir} → {dest_dir}")

    stats = {"copied": 0, "compressed": 0, "failed": 0}

    for root, dirs, files in os.walk(src_dir):
        for fname in files:
            src_path = os.path.join(root, fname)

            dest_file = os.path.join(dest_dir, fname)
            if compress:
                dest_file += ".gz"

            if compress:
                success = compress_file(src_path, dest_file)
                if success:
                    stats["compressed"] += 1
                    logging.info(f"✅ Compressed {src_path} → {dest_file}")
                else:
                    stats["failed"] += 1
            else:
                success = copy_file(src_path, dest_file)
                if success:
                    stats["copied"] += 1
                    logging.info(f"✅ Copied {src_path} → {dest_file}")
                else:
                    stats["failed"] += 1

    logging.info(
        f"✔ Finished backup for [{node_name}] "
        f"(Copied: {stats['copied']}, Compressed: {stats['compressed']}, Failed: {stats['failed']})"
    )

    return stats


# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    try:
        os.makedirs(backup_root, exist_ok=True)
    except Exception as e:
        logging.critical(f"❌ Cannot create root backup directory {backup_root}: {e}")
        exit(1)

    total_stats = {"copied": 0, "compressed": 0, "failed": 0}

    for node in nodes:
        node_stats = backup_node(node)
        for k in total_stats:
            total_stats[k] += node_stats[k]

    logging.info(
        f"📊 Backup Summary → Copied: {total_stats['copied']}, "
        f"Compressed: {total_stats['compressed']}, Failed: {total_stats['failed']}"
    )
