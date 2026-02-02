import csv
import logging
import os
from datetime import datetime
from pathlib import Path

from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from configuration import Configuration, Mode
from file_matcher import FileMatcher
from ftp_client import FileInfo, create_client


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.config = Configuration(**self.configuration.parameters)
        self._client = create_client(
            protocol=self.config.connection.protocol,
            hostname=self.config.connection.hostname,
            port=self.config.connection.port,
            user=self.config.connection.user,
            password=self.config.connection.password,
            ssh_config=self.config.connection.ssh if self.config.connection.protocol.value == "sftp" else None,
            passphrase=self.config.connection.passphrase,
            passive_mode=self.config.connection.passive_mode,
            connection_timeout=self.config.connection.connection_timeout,
            max_retries=self.config.connection.max_retries,
            base_path=self.config.connection.base_path,
        )

    def run(self):
        protocol = self.config.connection.protocol.value.upper()
        hostname = self.config.connection.hostname
        logging.info(f"Starting extraction from {protocol} server: {hostname}")

        previous_state = self.get_state_file() or {}
        last_extraction_time = previous_state.get("last_extraction_time", 0)

        try:
            self._client.connect()
        except Exception as e:
            raise UserException(f"Failed to connect to server: {str(e)}")

        try:
            files_to_extract = self._get_files_to_extract(self.config)

            if not files_to_extract:
                logging.warning("No files found matching the selection criteria")
                return

            if self.config.incremental_mode and last_extraction_time:
                logging.info(f"Incremental mode: filtering files modified after {last_extraction_time}")
                matcher = FileMatcher(self._client)
                files_to_extract = matcher.filter_by_modification_time(files_to_extract, last_extraction_time)

                if not files_to_extract:
                    logging.info("No new or modified files found")
                    return

            logging.info(f"Found {len(files_to_extract)} file(s) to extract")

            if self.config.mode == Mode.file:
                extracted_files = self._extract_files(files_to_extract, self.config)
                self._write_file_manifests(extracted_files, self.config.tags)
                files_count = len(extracted_files)
            else:  # table mode
                extracted_table = self._extract_table(files_to_extract[0], self.config)
                self._write_table_manifest(extracted_table, self.config)
                files_count = 1

            new_state = {"last_extraction_time": datetime.now().timestamp(), "files_extracted": files_count}
            self.write_state_file(new_state)

            if self.config.mode == Mode.file:
                logging.info(f"Successfully extracted {files_count} file(s)")
            else:
                logging.info(f"Successfully extracted table: {extracted_table}")

        finally:
            if self._client:
                self._client.disconnect()

    @sync_action("testConnection")
    def test_connection(self):
        protocol = self.config.connection.protocol.value.upper()
        hostname = self.config.connection.hostname
        logging.info(f"Testing connection to {protocol} server: {hostname}")

        try:
            self._client.connect()
            self._client.disconnect()
            logging.info("Connection test successful")
            return {"status": "success", "message": "Connection successful"}
        except Exception as e:
            error_msg = f"Connection test failed: {str(e)}"
            logging.error(error_msg)
            raise UserException(error_msg)

    @sync_action("list_files")
    def list_files(self):
        logging.info("Listing files for selection")

        try:
            self._client.connect()

            try:
                if self.config.connection.protocol.value == "sftp":
                    list_path = "."
                else:
                    list_path = "/"

                files = self._client.list_files(list_path, recursive=True)
                return [SelectElement(file.path) for file in files]

            finally:
                self._client.disconnect()

        except Exception as e:
            error_msg = f"Failed to list files: {str(e)}"
            logging.error(error_msg)
            raise UserException(error_msg)

    @sync_action("load_csv_columns")
    def load_csv_columns(self):
        logging.info("Loading CSV columns from selected file")

        try:
            # Get the file path from table_file (table mode) or first file from files array
            file_path = None
            if hasattr(self.config, "table_file") and self.config.table_file:
                file_path = self.config.table_file
            elif self.config.files and len(self.config.files) > 0:
                file_path = self.config.files[0]

            if not file_path:
                raise UserException("No file selected. Please select a file first.")

            self._client.connect()

            try:
                # Download the first few bytes to read the header
                import io

                buffer = io.BytesIO()

                # Try to download just the first 8KB which should contain the header
                try:
                    self._client.download_file(file_path, buffer, max_bytes=8192)
                except Exception:
                    # If max_bytes is not supported, download the whole file
                    self._client.download_file(file_path, buffer)

                buffer.seek(0)

                # Read the first line as CSV header
                import csv

                content = buffer.read().decode("utf-8")
                reader = csv.reader(io.StringIO(content))
                header = next(reader)

                return [SelectElement(col) for col in header]

            finally:
                self._client.disconnect()

        except Exception as e:
            error_msg = f"Failed to load CSV columns: {str(e)}"
            logging.error(error_msg)
            raise UserException(error_msg)

    def _get_files_to_extract(self, params: Configuration) -> list[FileInfo]:
        matcher = FileMatcher(self._client)

        # In table mode, use table_file or fallback to files[0]
        if params.mode == Mode.table:
            file_path = params.table_file or (params.files[0] if params.files else None)
            if not file_path:
                raise UserException("No file specified for table mode")
            return matcher.match_multiple_patterns([file_path])

        return matcher.match_multiple_patterns(params.files)

    def _extract_files(self, files: list[FileInfo], params: Configuration) -> list[str]:
        output_dir = Path(self.data_folder_path) / "out" / "files"
        output_dir.mkdir(parents=True, exist_ok=True)

        extracted_files = []

        for file_info in files:
            try:
                output_filename = self._get_output_filename(
                    file_info,
                    params.include_path_in_filename,
                    params.append_timestamp,
                )

                output_path = output_dir / output_filename

                logging.info(f"Downloading {file_info.path} to {output_filename}")
                with open(output_path, "wb") as f:
                    self._client.download_file(file_info.path, f)

                extracted_files.append(output_filename)

            except Exception as e:
                logging.error(f"Failed to extract {file_info.path}: {str(e)}")

        return extracted_files

    def _get_output_filename(self, file_info: FileInfo, include_path: bool, append_timestamp: bool) -> str:
        if include_path:
            filename = file_info.path.replace("/", "_").replace("\\", "_").lstrip("_")
        else:
            filename = os.path.basename(file_info.path)

        if append_timestamp:
            name_parts = os.path.splitext(filename)
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            filename = f"{name_parts[0]}_{timestamp}{name_parts[1]}"

        return filename

    def _extract_table(self, file_info: FileInfo, params: Configuration) -> str:
        """Extract a single file to the tables directory."""
        output_dir = Path(self.data_folder_path) / "out" / "tables"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Use table_name from destination, fallback to original filename
        table_name = params.destination.table_name or os.path.splitext(os.path.basename(file_info.path))[0]
        # Ensure .csv extension
        if not table_name.endswith(".csv"):
            table_name = f"{table_name}.csv"

        output_path = output_dir / table_name

        logging.info(f"Downloading {file_info.path} to table {table_name}")
        try:
            with open(output_path, "wb") as f:
                self._client.download_file(file_info.path, f)
            return table_name
        except Exception as e:
            logging.error(f"Failed to extract {file_info.path}: {str(e)}")
            raise UserException(f"Failed to extract table: {str(e)}")

    def _write_file_manifests(self, filenames: list[str], tags: list[str]) -> None:
        """Write manifests for files in file mode."""
        for filename in filenames:
            output_file = self.create_out_file_definition(
                name=filename, tags=tags if tags else [], is_public=False, is_permanent=True
            )
            self.write_manifest(output_file)

    def _get_csv_columns(self, table_path: Path) -> list[str]:
        """Read column names from CSV file header."""
        try:
            with open(table_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader)
                return header
        except Exception as e:
            logging.error(f"Failed to read CSV header from {table_path}: {str(e)}")
            raise UserException(f"Failed to read CSV header: {str(e)}")

    def _write_table_manifest(self, table_name: str, params: Configuration) -> None:
        """Write manifest for table in table mode."""
        # Determine columns for schema
        if params.has_header:
            # Read columns from CSV header
            table_path = Path(self.data_folder_path) / "out" / "tables" / table_name
            columns = self._get_csv_columns(table_path)
        else:
            # Use manually defined columns
            columns = params.destination.columns

        # Create table definition without has_header parameter first
        output_table = self.create_out_table_definition(
            name=table_name,
            primary_key=params.destination.primary_key,
            incremental=params.destination.incremental,
            columns=columns,
            has_header=params.has_header,
        )

        self.write_manifest(output_table)


if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
