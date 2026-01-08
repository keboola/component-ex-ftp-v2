import unittest

from datadirtest import DataDirTester
from freezegun import freeze_time


class TestComponent(unittest.TestCase):
    @freeze_time("2023-04-02")
    def test_functional_ftp(self):
        functional_tests = DataDirTester(data_dir="./tests/test_functional_ftp")
        functional_tests.run()

    @freeze_time("2023-04-02")
    def test_functional_sftp(self):
        functional_tests = DataDirTester(data_dir="./tests/test_functional_sftp")
        functional_tests.run()


if __name__ == "__main__":
    unittest.main()
