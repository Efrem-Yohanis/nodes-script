import os
import shutil
import time
import paramiko
import stat

# --- SFTP Configuration ---
HOST = 'localhost'
PORT = 2222
USERNAME = 'foo'
PASSWORD = 'pass'
REMOTE_ROOT_DIR = 'expected'    # Root folder on SFTP
LOCAL_IN_DIR = './in'           # Local folder to store files
BACKUP_DIR = './backup'         # Backup folder
REMOTE_SUFFIX = '.done'         # Rename remote files after collection
DELETE_SOURCE = False           # Keep remote files after collection
STATE_FILE = 'last_collected.txt'  # Track last processed batch

# Ensure local folders exist
os.makedirs(LOCAL_IN_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)


def connect_sftp():
    transport = paramiko.Transport((HOST, PORT))
    transport.connect(username=USERNAME, password=PASSWORD)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport


def get_last_collected():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return f.read().strip()
    return ''


def save_last_collected(folder_name):
    with open(STATE_FILE, 'w') as f:
        f.write(folder_name)


def sftp_walk(sftp, remotepath):
    """Recursively walk remote SFTP directory like os.walk"""
    folders, files = [], []
    try:
        for f in sftp.listdir_attr(remotepath):
            fname = f.filename
            if stat.S_ISDIR(f.st_mode):
                folders.append(fname)
            else:
                files.append(fname)
    except FileNotFoundError:
        return

    yield remotepath, folders, files
    for folder in folders:
        new_path = f"{remotepath}/{folder}"
        yield from sftp_walk(sftp, new_path)


def collect_files():
    sftp, transport = connect_sftp()
    try:
        try:
            remote_folders = sorted(sftp.listdir(REMOTE_ROOT_DIR))
        except FileNotFoundError:
            print("Remote root directory not found.")
            return

        last_collected = get_last_collected()

        for folder in remote_folders:
            if folder <= last_collected:
                continue  # skip already collected

            remote_folder_path = f"{REMOTE_ROOT_DIR}/{folder}"

            for root_dir, _, files in sftp_walk(sftp, remote_folder_path):
                # Compute relative path from remote root
                rel_path = os.path.relpath(root_dir, REMOTE_ROOT_DIR)

                # Create same folder structure locally
                local_dir = os.path.join(LOCAL_IN_DIR, rel_path)
                os.makedirs(local_dir, exist_ok=True)

                # Create same folder structure in backup
                backup_dir = os.path.join(BACKUP_DIR, rel_path)
                os.makedirs(backup_dir, exist_ok=True)

                for file in files:
                    remote_file_path = f"{root_dir}/{file}"
                    local_file_path = os.path.join(local_dir, file)
                    backup_file_path = os.path.join(backup_dir, file)

                    if os.path.exists(local_file_path):
                        print(f"Already collected: {file}")
                        continue

                    try:
                        print(f"Downloading {rel_path}/{file}")
                        sftp.get(remote_file_path, local_file_path)
                        print(f"✅ Downloaded: {file}")

                        # Backup
                        shutil.copy2(local_file_path, backup_file_path)

                        # Rename remote file safely
                        if not remote_file_path.endswith(REMOTE_SUFFIX):
                            remote_done_path = remote_file_path + REMOTE_SUFFIX
                            sftp.rename(remote_file_path, remote_done_path)
                            print(f"Renamed remote file to: {remote_done_path}")

                        # Optionally delete
                        if DELETE_SOURCE:
                            sftp.remove(remote_done_path)
                            print(f"Deleted remote file: {remote_done_path}")

                    except PermissionError as e:
                        print(f"Permission denied for {remote_file_path}: {e}")
                    except Exception as e:
                        print(f"Error collecting {remote_file_path}: {e}")

            # Update last collected folder
            save_last_collected(folder)

    finally:
        sftp.close()
        transport.close()


if __name__ == "__main__":
    while True:
        try:
            collect_files()
        except Exception as e:
            print("Error in collection process:", e)
        time.sleep(1)
