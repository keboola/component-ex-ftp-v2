"""Tests for FTP timezone handling — synchronize_times and MDTM calibration."""

import ftplib
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import ftputil.error

from configuration import Protocol
from file_matcher import FileMatcher
from ftp_client import FTPClient, FileInfo


class TestSynchronizeTimes(unittest.TestCase):
    @patch("ftp_client.ftputil.session.session_factory")
    @patch("ftp_client.ftputil.FTPHost")
    def test_success_sets_time_synced(self, mock_ftp_host_cls, mock_sf):
        mock_host = MagicMock()
        mock_host.time_shift.return_value = 3600.0
        mock_ftp_host_cls.return_value = mock_host

        client = FTPClient("host", 21, "user", "pass", Protocol.FTP)
        client.connect()

        mock_host.synchronize_times.assert_called_once()
        assert client._time_synced is True

    @patch("ftp_client.ftputil.session.session_factory")
    @patch("ftp_client.ftputil.FTPHost")
    def test_time_shift_error_leaves_unsynced(self, mock_ftp_host_cls, mock_sf):
        mock_host = MagicMock()
        mock_host.synchronize_times.side_effect = ftputil.error.TimeShiftError("ro")
        mock_ftp_host_cls.return_value = mock_host

        client = FTPClient("host", 21, "user", "pass", Protocol.FTP)
        client.connect()

        assert client._time_synced is False

    @patch("ftp_client.ftputil.session.session_factory")
    @patch("ftp_client.ftputil.FTPHost")
    def test_ftp_error_leaves_unsynced(self, mock_ftp_host_cls, mock_sf):
        mock_host = MagicMock()
        mock_host.synchronize_times.side_effect = ftputil.error.FTPError("err")
        mock_ftp_host_cls.return_value = mock_host

        client = FTPClient("host", 21, "user", "pass", Protocol.FTP)
        client.connect()

        assert client._time_synced is False


class TestCalibrateTimeShift(unittest.TestCase):
    def _make_client(self) -> FTPClient:
        client = FTPClient("host", 21, "user", "pass", Protocol.FTP)
        client._ftp_host = MagicMock()
        client._time_synced = False
        return client

    def test_computes_shift_from_mdtm(self):
        client = self._make_client()
        mdtm_ts = datetime(2026, 4, 7, 10, 55, 0, tzinfo=timezone.utc).timestamp()
        stat_mtime = mdtm_ts + 3600  # server is UTC+1

        client._ftp_host._session.sendcmd.return_value = "213 20260407105500"
        client._calibrate_time_shift("/test.txt", stat_mtime)

        client._ftp_host.set_time_shift.assert_called_once_with(3600)
        assert client._time_synced is True

    def test_rounds_shift_to_nearest_minute(self):
        client = self._make_client()
        mdtm_ts = datetime(2026, 4, 7, 10, 55, 0, tzinfo=timezone.utc).timestamp()
        stat_mtime = mdtm_ts + 3623  # jitter

        client._ftp_host._session.sendcmd.return_value = "213 20260407105500"
        client._calibrate_time_shift("/test.txt", stat_mtime)

        client._ftp_host.set_time_shift.assert_called_once_with(3600)

    def test_mdtm_unsupported_skips_set_time_shift(self):
        client = self._make_client()
        client._ftp_host._session.sendcmd.side_effect = ftplib.error_perm("500")

        client._calibrate_time_shift("/test.txt", 123456.0)

        client._ftp_host.set_time_shift.assert_not_called()
        assert client._time_synced is True  # won't retry

    def test_only_runs_once(self):
        client = self._make_client()
        client._ftp_host._session.sendcmd.return_value = "213 20260407105500"

        client._calibrate_time_shift("/a.txt", 100.0)
        client._calibrate_time_shift("/b.txt", 200.0)

        assert client._ftp_host._session.sendcmd.call_count == 1

    def test_noop_when_already_synced(self):
        client = self._make_client()
        client._time_synced = True

        client._calibrate_time_shift("/test.txt", 100.0)

        client._ftp_host._session.sendcmd.assert_not_called()


class TestFilterWithUTCAwareDatetimes(unittest.TestCase):
    def test_filters_correctly(self):
        threshold = datetime(2026, 3, 22, 3, 31, 41, tzinfo=timezone.utc).timestamp()
        files = [
            FileInfo("/old.csv", "old.csv", 100, datetime(2026, 3, 1, tzinfo=timezone.utc), False),
            FileInfo("/new.csv", "new.csv", 200, datetime(2026, 4, 7, 10, 55, tzinfo=timezone.utc), False),
            FileInfo("/exact.csv", "exact.csv", 150, datetime(2026, 3, 22, 3, 31, 41, tzinfo=timezone.utc), False),
        ]

        filtered = FileMatcher(client=MagicMock()).filter_by_modification_time(files, threshold)

        assert len(filtered) == 1
        assert filtered[0].path == "/new.csv"

    def test_original_bug_scenario(self):
        """CFTL-457: CET server + wrong year from ftputil."""
        last = datetime(2026, 3, 22, 3, 31, 41, tzinfo=timezone.utc).timestamp()
        matcher = FileMatcher(client=MagicMock())

        correct = FileInfo("/d.csv", "d.csv", 100, datetime(2026, 4, 7, 8, 55, tzinfo=timezone.utc), False)
        broken = FileInfo("/d.csv", "d.csv", 100, datetime(2025, 4, 7, 10, 55, tzinfo=timezone.utc), False)

        assert len(matcher.filter_by_modification_time([correct], last)) == 1
        assert len(matcher.filter_by_modification_time([broken], last)) == 0


if __name__ == "__main__":
    unittest.main()
