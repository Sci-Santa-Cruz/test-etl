import os
import hashlib
import logging
from datetime import datetime
from typing import List
import paramiko
from pathlib import Path

logger = logging.getLogger(__name__)

class SFTPExtractor:
    def __init__(self, host: str, port: int, username: str, password: str, remote_dir: str, local_dir: str):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.remote_dir = remote_dir
        self.local_dir = local_dir
        Path(local_dir).mkdir(parents=True, exist_ok=True)

    def connect(self):
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(self.host, port=self.port, username=self.username, password=self.password)
        self.sftp = self.ssh.open_sftp()

    def disconnect(self):
        if hasattr(self, 'sftp'):
            self.sftp.close()
        if hasattr(self, 'ssh'):
            self.ssh.close()

    def list_files(self) -> List[str]:
        """List .txt files in remote directory"""
        files = []
        for filename in self.sftp.listdir(self.remote_dir):
            if filename.endswith('.txt'):
                files.append(filename)
        return files

    def download_file(self, filename: str) -> str:
        """Download file and return local path"""
        remote_path = os.path.join(self.remote_dir, filename)
        local_path = os.path.join(self.local_dir, filename)
        self.sftp.get(remote_path, local_path)
        logger.info(f"Downloaded {filename} to {local_path}")
        return local_path

    def get_file_hash(self, filepath: str) -> str:
        """Calculate SHA256 hash of file"""
        hash_sha256 = hashlib.sha256()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()

    def extract_files(self) -> List[str]:
        """Main extraction method"""
        downloaded_files = []
        try:
            self.connect()
            files = self.list_files()
            for filename in files:
                local_path = self.download_file(filename)
                file_hash = self.get_file_hash(local_path)
                # Log extraction details
                file_size = os.path.getsize(local_path)
                logger.info(f"File: {filename}, Size: {file_size}, Hash: {file_hash}, Downloaded at: {datetime.now()}")
                downloaded_files.append(local_path)
        finally:
            self.disconnect()
        return downloaded_files