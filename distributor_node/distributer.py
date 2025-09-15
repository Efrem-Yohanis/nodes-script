import os
import shutil
import paramiko
import hashlib
import gzip
import time
import logging

# ----------------------------
# Logging configuration
# ----------------------------
log_file = "sftp_delivery.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# ----------------------------
# Node Parameters (One Node: 6D)
# ----------------------------
node = {
    "name": "6D",
    "host": "localhost",
    "port": 2225,
    "username": "d6_user",
    "password": "d6_pass",
    "target_dir": "/BSS_App_Data/bss-nfs-cdr/MED/dumper/dumpinput",
    "remote_prefix": "",
    "remote_suffix": ".done",
    "retries": 3,
    "retry_interval": 5,
    "compression": False
}

# Global paths
output_folder = "in"      # put your test files here
backup_folder = "backup"  # delivered files backed up here
temp_suffix = ".tmp"

# ----------------------------
# Utility Functions
# ----------------------------
def compute_md5(file_path):
    """Compute MD5 checksum of a file"""
    import hashlib
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def compress_file(file_path):
    """Optional gzip compression"""
    compressed_path = file_path + ".gz"
    with open(file_path, "rb") as f_in, gzip.open(compressed_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    return compressed_path

def get_all_files(folder):
    """List all files in a folder"""
    if not os.path.exists(folder):
        return []
    all_files = []
    for root, dirs, files in os.walk(folder):
        for f in files:
            all_files.append(os.path.join(root, f))
    return all_files

# ----------------------------
# SFTP Transfer
# ----------------------------
def sftp_transfer(local_file):
    file_to_send = local_file
    if node["compression"]:
        file_to_send = compress_file(local_file)

    # build remote names
    base_name = os.path.basename(file_to_send)
    remote_file = f"{node['remote_prefix']}{base_name}{node['remote_suffix']}"
    remote_temp_file = f"{base_name}{temp_suffix}"

    logging.info(f"Preparing transfer: {local_file} → {node['name']}:{node['target_dir']}")

    for attempt in range(1, node["retries"] + 1):
        try:
            # connect
            transport = paramiko.Transport((node["host"], node["port"]))
            transport.connect(username=node["username"], password=node["password"])
            sftp = paramiko.SFTPClient.from_transport(transport)

            # ensure target dir exists
            try:
                sftp.chdir(node["target_dir"])
            except IOError:
                sftp.mkdir(node["target_dir"])
                sftp.chdir(node["target_dir"])

            # upload temp file
            logging.info(f"Uploading {file_to_send} as {remote_temp_file}")
            sftp.put(file_to_send, os.path.join(node["target_dir"], remote_temp_file))

            # rename to final file
            sftp.rename(
                os.path.join(node["target_dir"], remote_temp_file),
                os.path.join(node["target_dir"], remote_file)
            )

            sftp.close()
            transport.close()

            # backup locally
            os.makedirs(backup_folder, exist_ok=True)
            shutil.copy(local_file, backup_folder)
            logging.info(f"✅ SUCCESS: {local_file} delivered to {node['name']}")

            return True

        except Exception as e:
            logging.error(f"❌ Attempt {attempt} failed: {e}")
            if attempt < node["retries"]:
                logging.info(f"Retrying in {node['retry_interval']} seconds...")
                time.sleep(node["retry_interval"])
            else:
                logging.error(f"FAILED: Could not deliver {local_file} to {node['name']}")
                return False

# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    files = get_all_files(output_folder)
    if not files:
        logging.warning(f"No files found in {output_folder}")
    else:
        for f in files:
            sftp_transfer(f)
