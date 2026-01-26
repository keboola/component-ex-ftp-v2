import os
import tempfile
import unittest

import mock
from freezegun import freeze_time
from keboola.component.exceptions import UserException

from component import Component
from configuration import Configuration, Mode


class TestComponent(unittest.TestCase):
    @freeze_time("2010-10-10")
    @mock.patch.dict(os.environ, {"KBC_DATADIR": "./non-existing-dir"})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()

    def test_list_all_files_sync_action(self):
        """Test list_all_files sync action against FTP server in docker-compose"""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "connection": {
                    "protocol": "ftp",
                    "hostname": "ftp",
                    "port": 21,
                    "user": "testuser",
                    "#pass": "testpass",
                    "passive_mode": True,
                    "connection_timeout": 30,
                    "max_retries": 2,
                },
                "debug": False,
            }

            os.makedirs(f"{tmpdir}/config", exist_ok=True)
            import json

            with open(f"{tmpdir}/config.json", "w") as f:
                json.dump({"parameters": config}, f)

            with mock.patch.dict(os.environ, {"KBC_DATADIR": tmpdir}):
                comp = Component()
                result = comp.list_all_files()

                self.assertIsInstance(result, list)
                self.assertGreater(len(result), 0)

                file_paths = [item.value for item in result]
                self.assertIn("/days.csv", file_paths)
                self.assertIn("/sliced/a.csv", file_paths)
                self.assertIn("/sliced/b.csv", file_paths)

    def test_table_mode_validation_file_required(self):
        """Test that table mode requires a file path (table_file or files)"""
        config = {
            "connection": {
                "protocol": "ftp",
                "hostname": "ftp",
                "port": 21,
                "user": "testuser",
                "#pass": "testpass",
            },
            "mode": "table",
            "table_file": "",  # No table_file
            "files": [],  # No files either
            "destination": {"table_name": "test_table"},
        }

        with self.assertRaises(UserException) as context:
            Configuration(**config)
        self.assertIn("exactly one file path", str(context.exception))

    def test_table_mode_validation_no_wildcards(self):
        """Test that table mode does not allow wildcards"""
        config = {
            "connection": {
                "protocol": "ftp",
                "hostname": "ftp",
                "port": 21,
                "user": "testuser",
                "#pass": "testpass",
            },
            "mode": "table",
            "table_file": "*.csv",  # Wildcards not allowed
            "destination": {"table_name": "test_table"},
        }

        with self.assertRaises(UserException) as context:
            Configuration(**config)
        self.assertIn("Wildcards are not allowed", str(context.exception))

    def test_table_mode_optional_table_name(self):
        """Test that table mode works without table_name (uses filename as fallback)"""
        config = {
            "connection": {
                "protocol": "ftp",
                "hostname": "ftp",
                "port": 21,
                "user": "testuser",
                "#pass": "testpass",
            },
            "mode": "table",
            "files": ["data/books.csv"],
            "destination": {},  # No table_name specified, will use filename
        }

        cfg = Configuration(**config)
        self.assertEqual(cfg.mode, Mode.table)
        self.assertEqual(cfg.destination.table_name, "")  # Empty, will be set during extraction

    def test_file_mode_default(self):
        """Test that file mode is the default"""
        config = {
            "connection": {
                "protocol": "ftp",
                "hostname": "ftp",
                "port": 21,
                "user": "testuser",
                "#pass": "testpass",
            },
            "files": ["*.csv"],
        }

        cfg = Configuration(**config)
        self.assertEqual(cfg.mode, Mode.file)

    def test_table_mode_config_valid(self):
        """Test valid table mode configuration with table_file"""
        config = {
            "connection": {
                "protocol": "ftp",
                "hostname": "ftp",
                "port": 21,
                "user": "testuser",
                "#pass": "testpass",
            },
            "mode": "table",
            "table_file": "data/books.csv",
            "destination": {
                "table_name": "books",
                "load_type": "incremental_load",
                "primary_key": ["id"],
            },
        }

        cfg = Configuration(**config)
        self.assertEqual(cfg.mode, Mode.table)
        self.assertEqual(cfg.table_file, "data/books.csv")
        self.assertEqual(cfg.destination.table_name, "books")
        self.assertEqual(cfg.destination.primary_key, ["id"])
        self.assertTrue(cfg.destination.incremental)

    def test_table_mode_config_fallback_to_files(self):
        """Test that table mode falls back to files[0] if table_file not set"""
        config = {
            "connection": {
                "protocol": "ftp",
                "hostname": "ftp",
                "port": 21,
                "user": "testuser",
                "#pass": "testpass",
            },
            "mode": "table",
            "files": ["data/books.csv"],
            "destination": {
                "table_name": "books",
                "load_type": "incremental_load",
                "primary_key": ["id"],
            },
        }

        cfg = Configuration(**config)
        self.assertEqual(cfg.mode, Mode.table)
        # Validation should pass using files[0] as fallback
        self.assertEqual(cfg.files[0], "data/books.csv")

    def test_table_mode_has_header_default_true(self):
        """Test that has_header defaults to True"""
        config = {
            "connection": {
                "protocol": "ftp",
                "hostname": "ftp",
                "port": 21,
                "user": "testuser",
                "#pass": "testpass",
            },
            "mode": "table",
            "files": ["data/books.csv"],
            "destination": {
                "table_name": "books",
            },
        }

        cfg = Configuration(**config)
        self.assertTrue(cfg.has_header)

    def test_table_mode_has_header_false_requires_columns(self):
        """Test that has_header=False requires columns to be defined"""
        config = {
            "connection": {
                "protocol": "ftp",
                "hostname": "ftp",
                "port": 21,
                "user": "testuser",
                "#pass": "testpass",
            },
            "mode": "table",
            "files": ["data/books.csv"],
            "has_header": False,
            "destination": {
                "table_name": "books",
                # No columns defined - should fail
            },
        }

        with self.assertRaises(UserException) as context:
            Configuration(**config)
        self.assertIn("columns must be defined", str(context.exception))

    def test_table_mode_has_header_false_with_columns(self):
        """Test valid configuration with has_header=False and columns defined"""
        config = {
            "connection": {
                "protocol": "ftp",
                "hostname": "ftp",
                "port": 21,
                "user": "testuser",
                "#pass": "testpass",
            },
            "mode": "table",
            "files": ["data/books.csv"],
            "has_header": False,
            "destination": {
                "table_name": "books",
                "columns": ["col1", "col2", "col3"],
                "primary_key": ["col1"],
            },
        }

        cfg = Configuration(**config)
        self.assertFalse(cfg.has_header)
        self.assertEqual(cfg.destination.columns, ["col1", "col2", "col3"])


if __name__ == "__main__":
    unittest.main()
