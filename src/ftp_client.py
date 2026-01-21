"""FTP client implementations for different protocols."""

import ftplib
import io
import logging
import os
import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import BinaryIO

import backoff
import ftputil
import ftputil.session
import paramiko
from keboola.component.exceptions import UserException

from configuration import SSH, Protocol


@dataclass
class FileInfo:
    """Information about a remote file."""

    path: str
    name: str
    size: int
    mtime: datetime
    is_dir: bool


class FTPClientBase(ABC):
    """Abstract base class for FTP clients."""

    MAX_RETRIES = 2

    def __init__(
        self, hostname: str, port: int, user: str, password: str, connection_timeout: int = 30, max_retries: int = 2
    ):
        self.hostname = hostname
        self.port = port
        self.user = user
        self.password = password
        self.connection_timeout = connection_timeout
        self.max_retries = max_retries
        self._connection = None
        self.logger = logging.getLogger(__name__)

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to FTP server."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to FTP server."""
        pass

    @abstractmethod
    def list_files(self, path: str, recursive: bool = False) -> list[FileInfo]:
        """List files in a directory.

        Args:
            path: Remote directory path
            recursive: If True, recursively list all files in subdirectories

        Returns:
            List of FileInfo objects
        """
        pass

    @abstractmethod
    def download_file(self, remote_path: str, local_file: BinaryIO) -> None:
        """Download a file from the server.

        Args:
            remote_path: Path to remote file
            local_file: File-like object to write downloaded data to
        """
        pass

    @abstractmethod
    def file_exists(self, remote_path: str) -> bool:
        """Check if a file exists on the server."""
        pass

    @abstractmethod
    def get_file_info(self, remote_path: str) -> FileInfo:
        """Get information about a specific file."""
        pass

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()


class SFTPClient(FTPClientBase):
    """SFTP client using Paramiko."""

    def __init__(
        self,
        hostname: str,
        port: int,
        user: str,
        password: str,
        ssh_config: SSH,
        passphrase: str = "",
        connection_timeout: int = 30,
        max_retries: int = 2,
        base_path: str = "",
    ):
        super().__init__(hostname, port, user, password, connection_timeout, max_retries)
        self.ssh_config = ssh_config
        self.passphrase = passphrase
        self.base_path = base_path
        self._transport: paramiko.Transport | None = None
        self._sftp: paramiko.SFTPClient | None = None
        self._actual_cwd: str = ""  # Actual current working directory on the server

    @backoff.on_exception(
        backoff.expo,
        (paramiko.SSHException, EOFError, OSError),
        max_tries=3,
        on_backoff=lambda details: logging.info(f"Retrying SFTP connection (attempt {details['tries']})..."),
        on_giveup=lambda details: logging.error("Failed to connect to SFTP server after retries"),
    )
    def connect(self) -> None:
        """Establish SFTP connection."""
        try:
            self.logger.info(f"Connecting to SFTP server {self.hostname}:{self.port}")

            # Create transport
            self._transport = paramiko.Transport(
                (self.hostname, self.port), disabled_algorithms=self.ssh_config.disabled_algorithms or None
            )
            self._transport.banner_timeout = self.ssh_config.banner_timeout

            # Prepare authentication
            pkey = None
            if self.ssh_config.private_key:
                pkey = self._parse_private_key(self.ssh_config.private_key, self.passphrase or None)

            # Connect
            self._transport.connect(username=self.user, password=self.password or None, pkey=pkey)

            # Create SFTP client
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)

            # Change to base path if specified
            if self.base_path:
                try:
                    self._sftp.chdir(self.base_path)
                    self.logger.info(f"Changed to base directory: {self.base_path}")
                except IOError as e:
                    raise UserException(f"Failed to change to base directory '{self.base_path}': {str(e)}")

            # Get the actual current working directory path on the server
            # This resolves the real path in chroot/jailed environments (e.g., AWS Transfer Family)
            try:
                self._actual_cwd = self._sftp.normalize(".") if not self.base_path else f"/{self.base_path.strip('/')}"
            except Exception:
                self._actual_cwd = f"/{self.base_path.strip('/')}" if self.base_path else ""

            self.logger.info("Successfully connected to SFTP server")

        except paramiko.AuthenticationException as e:
            raise UserException(f"SFTP authentication failed: {str(e)}")
        except paramiko.SSHException as e:
            raise UserException(f"SFTP connection error: {str(e)}")
        except Exception as e:
            raise UserException(f"Failed to connect to SFTP server: {str(e)}")

    def _parse_private_key(self, key_string: str, passphrase: str | None = None) -> paramiko.PKey:
        """Parse private key from string."""
        key_file = io.StringIO(key_string)

        # Try different key types
        key_types = [
            paramiko.RSAKey,
            paramiko.ECDSAKey,
            paramiko.Ed25519Key,
        ]

        for key_class in key_types:
            try:
                key_file.seek(0)
                return key_class.from_private_key(key_file, password=passphrase)
            except paramiko.SSHException:
                continue

        raise UserException("Unable to parse SSH private key. Unsupported key type or invalid format.")

    def disconnect(self) -> None:
        """Close SFTP connection."""
        if self._sftp:
            self._sftp.close()
            self._sftp = None
        if self._transport:
            self._transport.close()
            self._transport = None
        self.logger.info("Disconnected from SFTP server")

    def _normalize_path(self, path: str) -> str:
        """Normalize path for SFTP operations.

        Strips the actual CWD prefix since we're already in that directory context.
        Also maps the logical root / to . if base_path is set.
        """
        if not self._actual_cwd:
            return path

        # If it's the logical root, map to current directory
        if path == "/":
            return "."

        # Strip the CWD prefix from the path if present
        if path.startswith(self._actual_cwd):
            normalized = path[len(self._actual_cwd) :].lstrip("/")
            return normalized if normalized else "."

        # If it's an absolute path but not starting with _actual_cwd,
        # it might be relative to the logical root (backward compatibility)
        if path.startswith("/"):
            return path.lstrip("/")

        return path

    def list_files(self, path: str, recursive: bool = False) -> list[FileInfo]:
        """List files in a directory."""
        if not self._sftp:
            raise UserException("Not connected to SFTP server")

        path = self._normalize_path(path)
        files = []
        try:
            self._list_files_recursive(path, files, recursive)
        except IOError as e:
            raise UserException(f"Failed to list files in {path}: {str(e)}")

        return files

    def _list_files_recursive(self, path: str, files: list[FileInfo], recursive: bool) -> None:
        """Recursively list files in a directory."""
        try:
            for attr in self._sftp.listdir_attr(path):
                # Build relative path for SFTP operations (relative to CWD)
                # If path is ".", use just the filename
                # If path is absolute or starts with something, join them
                if path == ".":
                    item_path = attr.filename
                else:
                    item_path = f"{path.rstrip('/')}/{attr.filename}"

                # Build absolute path for return value (FileInfo.path)
                # This should be absolute on the server, UNLESS base_path is set
                # In that case we want paths relative to the logical root (base_path)
                if self.base_path:
                    # Return relative path for extraction consistency
                    full_path = item_path
                elif item_path.startswith("/"):
                    # Already absolute (e.g. if initial path was /)
                    full_path = item_path
                else:
                    # Relative to CWD, so prepend _actual_cwd
                    full_path = f"{self._actual_cwd.rstrip('/')}/{item_path}"

                is_dir = self._is_directory(attr)

                if is_dir and recursive:
                    self._list_files_recursive(item_path, files, recursive)
                elif not is_dir:
                    files.append(
                        FileInfo(
                            path=full_path,
                            name=attr.filename,
                            size=attr.st_size or 0,
                            mtime=datetime.fromtimestamp(attr.st_mtime) if attr.st_mtime else datetime.now(),
                            is_dir=False,
                        )
                    )

        except IOError:
            pass  # Path might not exist or not accessible, skip

    def _is_directory(self, attr: paramiko.SFTPAttributes) -> bool:
        """Check if SFTP attributes represent a directory."""
        import stat

        return stat.S_ISDIR(attr.st_mode) if attr.st_mode else False

    def download_file(self, remote_path: str, local_file: BinaryIO) -> None:
        """Download a file from SFTP server."""
        if not self._sftp:
            raise UserException("Not connected to SFTP server")

        remote_path = self._normalize_path(remote_path)
        try:
            self.logger.info(f"Downloading file: {remote_path}")
            remote_file = self._sftp.file(remote_path, "rb")
            while True:
                chunk = remote_file.read(32768)  # 32KB chunks
                if not chunk:
                    break
                local_file.write(chunk)
            remote_file.close()
        except IOError as e:
            raise UserException(f"Failed to download file {remote_path}: {str(e)}")

    def file_exists(self, remote_path: str) -> bool:
        """Check if a file exists on SFTP server."""
        if not self._sftp:
            raise UserException("Not connected to SFTP server")

        remote_path = self._normalize_path(remote_path)
        try:
            self._sftp.stat(remote_path)
            return True
        except IOError:
            return False

    def get_file_info(self, remote_path: str) -> FileInfo:
        """Get information about a specific file."""
        if not self._sftp:
            raise UserException("Not connected to SFTP server")

        # Keep original path for return value
        original_path = remote_path
        normalized_path = self._normalize_path(remote_path)
        try:
            attr = self._sftp.stat(normalized_path)
            return FileInfo(
                path=original_path,
                name=os.path.basename(original_path),
                size=attr.st_size or 0,
                mtime=datetime.fromtimestamp(attr.st_mtime) if attr.st_mtime else datetime.now(),
                is_dir=self._is_directory(attr),
            )
        except IOError as e:
            raise UserException(f"Failed to get file info for {original_path}: {str(e)}")


class ExplicitFTPS(ftplib.FTP_TLS):
    """Explicit FTPS connection (AUTH TLS)."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.encoding = "utf-8"


class ImplicitFTPS(ftplib.FTP_TLS):
    """Implicit FTPS connection (TLS from start)."""

    def __init__(self, *args, **kwargs):
        kwargs["context"] = kwargs.get("context", None)
        super().__init__(*args, **kwargs)
        self.encoding = "utf-8"
        # Implicit FTPS wraps the control channel immediately
        self.ssl_version = getattr(kwargs.get("context", None), "_maximum_version", None)


class FTPClient(FTPClientBase):
    """FTP/FTPS client using ftputil."""

    def __init__(
        self,
        hostname: str,
        port: int,
        user: str,
        password: str,
        protocol: Protocol,
        passive_mode: bool = True,
        connection_timeout: int = 30,
        max_retries: int = 2,
    ):
        super().__init__(hostname, port, user, password, connection_timeout, max_retries)
        self.protocol = protocol
        self.passive_mode = passive_mode
        self._ftp_host: ftputil.FTPHost | None = None

    @backoff.on_exception(
        backoff.expo,
        (ftplib.error_temp, ftplib.error_perm, OSError),
        max_tries=3,
        on_backoff=lambda details: logging.info(f"Retrying FTP connection (attempt {details['tries']})..."),
        on_giveup=lambda details: logging.error("Failed to connect to FTP server after retries"),
    )
    def connect(self) -> None:
        """Establish FTP/FTPS connection."""
        try:
            self.logger.info(f"Connecting to {self.protocol.value.upper()} server {self.hostname}:{self.port}")

            # Select base class based on protocol
            if self.protocol == Protocol.FTP:
                base_class = ftplib.FTP
            elif self.protocol == Protocol.EX_FTPS:
                base_class = ExplicitFTPS
            elif self.protocol == Protocol.IM_FTPS:
                base_class = ImplicitFTPS
            else:
                raise UserException(f"Unsupported FTP protocol: {self.protocol}")

            # Create session factory
            session_factory = ftputil.session.session_factory(
                base_class=base_class,
                port=self.port,
                use_passive_mode=self.passive_mode,
                encrypt_data_channel=True if self.protocol in [Protocol.EX_FTPS, Protocol.IM_FTPS] else False,
            )

            # Connect
            self._ftp_host = ftputil.FTPHost(self.hostname, self.user, self.password, session_factory=session_factory)

            self.logger.info(f"Successfully connected to {self.protocol.value.upper()} server")

        except ftplib.error_perm as e:
            error_msg = str(e)
            if "530" in error_msg or "auth" in error_msg.lower():
                raise UserException(f"FTP authentication failed: {error_msg}")
            raise UserException(f"FTP permission error: {error_msg}")
        except Exception as e:
            raise UserException(f"Failed to connect to FTP server: {str(e)}")

    def disconnect(self) -> None:
        """Close FTP connection."""
        if self._ftp_host:
            try:
                self._ftp_host.close()
            except Exception:
                pass
            self._ftp_host = None
        self.logger.info("Disconnected from FTP server")

    def list_files(self, path: str, recursive: bool = False) -> list[FileInfo]:
        """List files in a directory."""
        if not self._ftp_host:
            raise UserException("Not connected to FTP server")

        files = []
        try:
            self._list_files_recursive(path, files, recursive)
        except ftputil.error.FTPError as e:
            raise UserException(f"Failed to list files in {path}: {str(e)}")

        return files

    def _list_files_recursive(self, path: str, files: list[FileInfo], recursive: bool) -> None:
        """Recursively list files in a directory."""
        try:
            if not self._ftp_host.path.exists(path):
                return

            if self._ftp_host.path.isfile(path):
                # Path is a file, not a directory
                stat_result = self._ftp_host.stat(path)
                files.append(
                    FileInfo(
                        path=path,
                        name=self._ftp_host.path.basename(path),
                        size=stat_result.st_size,
                        mtime=datetime.fromtimestamp(stat_result.st_mtime),
                        is_dir=False,
                    )
                )
                return

            # Path is a directory
            for name in self._ftp_host.listdir(path):
                if name in [".", ".."]:
                    continue

                full_path = self._ftp_host.path.join(path, name)

                try:
                    is_dir = self._ftp_host.path.isdir(full_path)

                    if is_dir:
                        if recursive:
                            self._list_files_recursive(full_path, files, recursive)
                    else:
                        stat_result = self._ftp_host.stat(full_path)
                        files.append(
                            FileInfo(
                                path=full_path,
                                name=name,
                                size=stat_result.st_size,
                                mtime=datetime.fromtimestamp(stat_result.st_mtime),
                                is_dir=False,
                            )
                        )
                except ftputil.error.FTPError:
                    # Skip files we can't access
                    continue

        except ftputil.error.FTPError:
            # Path doesn't exist or not accessible
            pass

    def download_file(self, remote_path: str, local_file: BinaryIO) -> None:
        """Download a file from FTP server."""
        if not self._ftp_host:
            raise UserException("Not connected to FTP server")

        try:
            self.logger.info(f"Downloading file: {remote_path}")
            with self._ftp_host.open(remote_path, "rb") as remote_file:
                shutil.copyfileobj(remote_file, local_file)
        except ftputil.error.FTPError as e:
            raise UserException(f"Failed to download file {remote_path}: {str(e)}")

    def file_exists(self, remote_path: str) -> bool:
        """Check if a file exists on FTP server."""
        if not self._ftp_host:
            raise UserException("Not connected to FTP server")

        return self._ftp_host.path.exists(remote_path)

    def get_file_info(self, remote_path: str) -> FileInfo:
        """Get information about a specific file."""
        if not self._ftp_host:
            raise UserException("Not connected to FTP server")

        try:
            stat_result = self._ftp_host.stat(remote_path)
            return FileInfo(
                path=remote_path,
                name=self._ftp_host.path.basename(remote_path),
                size=stat_result.st_size,
                mtime=datetime.fromtimestamp(stat_result.st_mtime),
                is_dir=self._ftp_host.path.isdir(remote_path),
            )
        except ftputil.error.FTPError as e:
            raise UserException(f"Failed to get file info for {remote_path}: {str(e)}")


def create_client(
    protocol: Protocol,
    hostname: str,
    port: int,
    user: str,
    password: str,
    ssh_config: SSH | None = None,
    passphrase: str = "",
    passive_mode: bool = True,
    connection_timeout: int = 30,
    max_retries: int = 2,
    base_path: str = "",
) -> FTPClientBase:
    """Factory function to create appropriate FTP client based on protocol.

    Args:
        protocol: FTP protocol type
        hostname: Server hostname
        port: Server port
        user: Username
        password: Password
        ssh_config: SSH configuration (required for SFTP)
        passphrase: Passphrase for encrypted SSH private key (SFTP only)
        passive_mode: Use passive mode for FTP/FTPS
        connection_timeout: Connection timeout in seconds
        max_retries: Maximum number of retry attempts

    Returns:
        FTP client instance
    """
    if protocol == Protocol.SFTP:
        if not ssh_config:
            ssh_config = SSH()
        return SFTPClient(
            hostname=hostname,
            port=port,
            user=user,
            password=password,
            ssh_config=ssh_config,
            passphrase=passphrase,
            connection_timeout=connection_timeout,
            max_retries=max_retries,
            base_path=base_path,
        )
    else:
        return FTPClient(
            hostname=hostname,
            port=port,
            user=user,
            password=password,
            protocol=protocol,
            passive_mode=passive_mode,
            connection_timeout=connection_timeout,
            max_retries=max_retries,
        )
