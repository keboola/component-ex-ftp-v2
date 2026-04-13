"""Tests for FTP timezone handling — synchronize_times, MDTM fallback, UTC-aware mtimes."""

import ftplib
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import ftputil.error

from configuration import Protocol
from file_matcher import FileMatcher
from ftp_client import FTPClient, FileInfo


class TestSynchronizeTimes(unittest.TestCase):
    """Tests for synchronize_times() call during FTP connect."""

    @patch("ftp_client.ftputil.session.session_factory")
    @patch("ftp_client.ftputil.FTPHost")
    def test_synchronize_times_success(self, mock_ftp_host_cls, mock_session_factory):
        mock_host = MagicMock()
        mock_host.time_shift.return_value = 3600.0
        mock_ftp_host_cls.return_value = mock_host

        client = FTPClient("host", 21, "user", "pass", Protocol.FTP)
        client.connect()

        mock_host.synchronize_times.assert_called_once()

    @patch("ftp_client.ftputil.session.session_factory")
    @patch("ftp_client.ftputil.FTPHost")
    def test_synchronize_times_time_shift_error_continues(
        self, mock_ftp_host_cls, mock_session_factory
    ):
        mock_host = MagicMock()
        mock_host.synchronize_times.side_effect = ftputil.error.TimeShiftError(
            "read-only"
        )
        mock_ftp_host_cls.return_value = mock_host

        client = FTPClient("host", 21, "user", "pass", Protocol.FTP)
        client.connect()  # Should not raise

        assert client._ftp_host is mock_host

    @patch("ftp_client.ftputil.session.session_factory")
    @patch("ftp_client.ftputil.FTPHost")
    def test_synchronize_times_ftp_error_continues(
        self, mock_ftp_host_cls, mock_session_factory
    ):
        mock_host = MagicMock()
        mock_host.synchronize_times.side_effect = ftputil.error.FTPError("some error")
        mock_ftp_host_cls.return_value = mock_host

        client = FTPClient("host", 21, "user", "pass", Protocol.FTP)
        client.connect()  # Should not raise

        assert client._ftp_host is mock_host


class TestMDTM(unittest.TestCase):
    """Tests for MDTM command fallback."""

    def _make_client(self) -> FTPClient:
        client = FTPClient("host", 21, "user", "pass", Protocol.FTP)
        client._ftp_host = MagicMock()
        return client

    def test_mdtm_parses_utc_timestamp(self):
        client = self._make_client()
        client._ftp_host._session.sendcmd.return_value = "213 20260407105500"

        result = client._get_mdtm_mtime("/test.txt")

        assert result == datetime(2026, 4, 7, 10, 55, 0, tzinfo=timezone.utc)
        assert client._mdtm_supported is True

    def test_mdtm_parses_fractional_seconds(self):
        client = self._make_client()
        client._ftp_host._session.sendcmd.return_value = "213 20260407105500.123"

        result = client._get_mdtm_mtime("/test.txt")

        assert result == datetime(2026, 4, 7, 10, 55, 0, tzinfo=timezone.utc)

    def test_mdtm_not_supported_caches_false(self):
        client = self._make_client()
        client._ftp_host._session.sendcmd.side_effect = ftplib.error_perm(
            "500 Unknown command"
        )

        result1 = client._get_mdtm_mtime("/test.txt")
        assert result1 is None
        assert client._mdtm_supported is False

        # Second call should not even try sendcmd again
        client._ftp_host._session.sendcmd.reset_mock()
        result2 = client._get_mdtm_mtime("/test2.txt")
        assert result2 is None
        client._ftp_host._session.sendcmd.assert_not_called()

    def test_mdtm_skipped_when_already_disabled(self):
        client = self._make_client()
        client._mdtm_supported = False

        result = client._get_mdtm_mtime("/test.txt")

        assert result is None
        client._ftp_host._session.sendcmd.assert_not_called()


class TestGetFileMtime(unittest.TestCase):
    """Tests for _get_file_mtime preference logic."""

    def _make_client(self) -> FTPClient:
        client = FTPClient("host", 21, "user", "pass", Protocol.FTP)
        client._ftp_host = MagicMock()
        return client

    def test_prefers_mdtm_over_stat(self):
        client = self._make_client()
        mdtm_dt = datetime(2026, 4, 7, 10, 55, 0, tzinfo=timezone.utc)
        client._ftp_host._session.sendcmd.return_value = "213 20260407105500"

        result = client._get_file_mtime("/test.txt", stat_mtime=0.0)

        assert result == mdtm_dt

    def test_falls_back_to_stat_when_mdtm_disabled(self):
        client = self._make_client()
        client._mdtm_supported = False

        stat_mtime = 1712487300.0  # 2024-04-07 10:55:00 UTC
        result = client._get_file_mtime("/test.txt", stat_mtime=stat_mtime)

        assert result == datetime.fromtimestamp(stat_mtime, tz=timezone.utc)
        assert result.tzinfo is timezone.utc


class TestFilterWithUTCAwareDatetimes(unittest.TestCase):
    """Tests for filter_by_modification_time with UTC-aware datetimes."""

    def test_filters_correctly_with_utc_aware_mtimes(self):
        threshold = datetime(2026, 3, 22, 3, 31, 41, tzinfo=timezone.utc).timestamp()

        files = [
            FileInfo(
                path="/old.csv",
                name="old.csv",
                size=100,
                mtime=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc),
                is_dir=False,
            ),
            FileInfo(
                path="/new.csv",
                name="new.csv",
                size=200,
                mtime=datetime(2026, 4, 7, 10, 55, tzinfo=timezone.utc),
                is_dir=False,
            ),
            FileInfo(
                path="/exact.csv",
                name="exact.csv",
                size=150,
                mtime=datetime(2026, 3, 22, 3, 31, 41, tzinfo=timezone.utc),
                is_dir=False,
            ),
        ]

        matcher = FileMatcher(client=MagicMock())
        filtered = matcher.filter_by_modification_time(files, threshold)

        # Only new.csv should pass (strict >), exact.csv is equal so excluded
        assert len(filtered) == 1
        assert filtered[0].path == "/new.csv"

    def test_the_original_bug_scenario(self):
        """Reproduce the exact scenario from CFTL-457.

        FTP server in CET (UTC+2) reports file mtime Apr 7 10:55 CET.
        Without fix, ftputil would interpret this as Apr 7 2025 (year subtracted).
        With fix (MDTM or synchronize_times), the mtime is correctly Apr 7 2026 10:55 UTC.
        """
        last_extraction = datetime(
            2026, 3, 22, 3, 31, 41, tzinfo=timezone.utc
        ).timestamp()

        # Correct mtime after fix: Apr 7 2026 08:55 UTC (10:55 CET = 08:55 UTC)
        correct_mtime = datetime(2026, 4, 7, 8, 55, 0, tzinfo=timezone.utc)

        # Broken mtime before fix: ftputil subtracted a year → Apr 7 2025
        broken_mtime = datetime(2025, 4, 7, 10, 55, 0, tzinfo=timezone.utc)

        file_correct = FileInfo(
            path="/data.csv",
            name="data.csv",
            size=100,
            mtime=correct_mtime,
            is_dir=False,
        )
        file_broken = FileInfo(
            path="/data.csv",
            name="data.csv",
            size=100,
            mtime=broken_mtime,
            is_dir=False,
        )

        matcher = FileMatcher(client=MagicMock())

        # With fix: file passes the filter
        assert (
            len(matcher.filter_by_modification_time([file_correct], last_extraction))
            == 1
        )

        # Without fix: file would be incorrectly filtered out
        assert (
            len(matcher.filter_by_modification_time([file_broken], last_extraction))
            == 0
        )


if __name__ == "__main__":
    unittest.main()
