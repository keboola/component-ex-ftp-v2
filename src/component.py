import logging
import os
from datetime import datetime
from pathlib import Path

from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from configuration import Configuration
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

            extracted_files = self._extract_files(files_to_extract, self.config)
            self._write_manifests(extracted_files, self.config.tags)

            new_state = {"last_extraction_time": datetime.now().timestamp(), "files_extracted": len(extracted_files)}
            self.write_state_file(new_state)

            logging.info(f"Successfully extracted {len(extracted_files)} file(s)")

        finally:
            if self._client:
                self._client.disconnect()

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

    @sync_action("list_all_files")
    def list_all_files(self):
        logging.info("Listing all files from server")

        try:
            self._client.connect()

            try:
                files = self._client.list_files("/", recursive=True)
                return [SelectElement(file.path) for file in files]

            finally:
                self._client.disconnect()

        except Exception as e:
            error_msg = f"Failed to list all files: {str(e)}"
            logging.error(error_msg)
            raise UserException(error_msg)

    def _get_files_to_extract(self, params: Configuration) -> list[FileInfo]:
        matcher = FileMatcher(self._client)
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

    def _write_manifests(self, filenames: list[str], tags: list[str]) -> None:
        for filename in filenames:
            output_file = self.create_out_file_definition(
                name=filename, tags=tags if tags else [], is_public=False, is_permanent=True
            )
            self.write_manifest(output_file)


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
