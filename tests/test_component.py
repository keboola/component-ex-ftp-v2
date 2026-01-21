import os
import tempfile
import unittest

import mock
from freezegun import freeze_time

from component import Component


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


if __name__ == "__main__":
    unittest.main()
