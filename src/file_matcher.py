"""File pattern matching for FTP file selection."""

import fnmatch
import logging
import os
from pathlib import PurePosixPath

from ftp_client import FileInfo, FTPClientBase


class FileMatcher:
    """Matches files based on patterns with support for wildcards and recursive matching."""

    def __init__(self, client: FTPClientBase):
        """Initialize file matcher.

        Args:
            client: Connected FTP client instance
        """
        self.client = client
        self.logger = logging.getLogger(__name__)

    def match_pattern(self, pattern: str) -> list[FileInfo]:
        """Match files based on a pattern.

        Supports:
        - Exact paths: /path/to/file.txt
        - Wildcards: /path/to/*.csv
        - Recursive wildcards: /path/**/file.txt or /path/**/*.csv

        Args:
            pattern: File pattern to match

        Returns:
            List of matching FileInfo objects
        """
        self.logger.info(f"Matching pattern: {pattern}")

        # Check if pattern contains ** (recursive wildcard)
        if "**" in pattern:
            return self._match_recursive_pattern(pattern)
        elif "*" in pattern or "?" in pattern:
            return self._match_wildcard_pattern(pattern)
        else:
            return self._match_exact_path(pattern)

    def _match_exact_path(self, path: str) -> list[FileInfo]:
        """Match an exact file path."""
        if self.client.file_exists(path):
            try:
                file_info = self.client.get_file_info(path)
                if not file_info.is_dir:
                    self.logger.info(f"Found exact match: {path}")
                    return [file_info]
            except Exception as e:
                self.logger.warning(f"Could not get info for {path}: {e}")
        return []

    def _match_wildcard_pattern(self, pattern: str) -> list[FileInfo]:
        """Match files using simple wildcards (* and ?) in a single directory."""
        # Split pattern into directory and filename pattern
        directory = os.path.dirname(pattern)
        filename_pattern = os.path.basename(pattern)

        if not directory:
            directory = "/"

        try:
            # List files in the directory
            all_files = self.client.list_files(directory, recursive=False)

            # Filter by pattern
            matched_files = [f for f in all_files if fnmatch.fnmatch(f.name, filename_pattern)]

            self.logger.info(f"Found {len(matched_files)} files matching {pattern}")
            return matched_files

        except Exception as e:
            self.logger.warning(f"Could not list files in {directory}: {e}")
            return []

    def _match_recursive_pattern(self, pattern: str) -> list[FileInfo]:
        """Match files using recursive wildcards (**)."""
        # Parse the pattern to extract base path and matching parts
        parts = pattern.split("**")

        if len(parts) > 2:
            # Multiple ** not supported in a simple way, use first occurrence
            base_path = parts[0].rstrip("/")
            remaining_pattern = "**".join(parts[1:]).lstrip("/")
        else:
            base_path = parts[0].rstrip("/")
            remaining_pattern = parts[1].lstrip("/") if len(parts) > 1 else ""

        if not base_path:
            base_path = "/"

        try:
            # List all files recursively from base path
            all_files = self.client.list_files(base_path, recursive=True)

            # Filter files based on remaining pattern
            if remaining_pattern:
                matched_files = []
                # If pattern is just a filename pattern (no /), match against basename
                if "/" not in remaining_pattern:
                    import fnmatch

                    for file_info in all_files:
                        if fnmatch.fnmatch(os.path.basename(file_info.path), remaining_pattern):
                            matched_files.append(file_info)
                else:
                    # Pattern contains path components, match full relative path
                    for file_info in all_files:
                        # Get relative path from base
                        if file_info.path.startswith(base_path):
                            rel_path = file_info.path[len(base_path) :].lstrip("/")
                        else:
                            rel_path = file_info.path

                        # Check if relative path matches the remaining pattern
                        if self._path_matches_pattern(rel_path, remaining_pattern):
                            matched_files.append(file_info)
            else:
                # No remaining pattern, return all files
                matched_files = all_files

            self.logger.info(f"Found {len(matched_files)} files matching {pattern}")
            return matched_files

        except Exception as e:
            self.logger.warning(f"Could not list files in {base_path}: {e}")
            return []

    def _path_matches_pattern(self, path: str, pattern: str) -> bool:
        """Check if a path matches a pattern.

        Args:
            path: Relative file path
            pattern: Pattern with wildcards

        Returns:
            True if path matches pattern
        """
        # Convert both to PurePosixPath for consistent handling
        path_parts = PurePosixPath(path).parts
        pattern_parts = PurePosixPath(pattern).parts

        return self._match_parts(path_parts, pattern_parts)

    def _match_parts(self, path_parts: tuple, pattern_parts: tuple) -> bool:
        """Recursively match path parts against pattern parts.

        Args:
            path_parts: Tuple of path components
            pattern_parts: Tuple of pattern components

        Returns:
            True if parts match
        """
        if not pattern_parts:
            return not path_parts  # Both empty = match

        if not path_parts:
            # Path exhausted, pattern should be empty or only wildcards
            return all(p == "*" for p in pattern_parts)

        pattern_part = pattern_parts[0]
        path_part = path_parts[0]

        # Check if current pattern part matches current path part
        if fnmatch.fnmatch(path_part, pattern_part):
            # Match, continue with remaining parts
            return self._match_parts(path_parts[1:], pattern_parts[1:])

        return False

    def match_multiple_patterns(self, patterns: list[str]) -> list[FileInfo]:
        """Match multiple patterns and return unique files.

        Args:
            patterns: List of file patterns

        Returns:
            List of unique matching FileInfo objects
        """
        all_matches = []
        seen_paths = set()

        for pattern in patterns:
            matches = self.match_pattern(pattern)
            for file_info in matches:
                if file_info.path not in seen_paths:
                    all_matches.append(file_info)
                    seen_paths.add(file_info.path)

        self.logger.info(f"Found {len(all_matches)} unique files from {len(patterns)} patterns")
        return all_matches

    def filter_by_modification_time(self, files: list[FileInfo], since: float) -> list[FileInfo]:
        """Filter files modified after a specific timestamp.

        Args:
            files: List of FileInfo objects
            since: Unix timestamp

        Returns:
            Filtered list of files
        """
        filtered = [f for f in files if f.mtime.timestamp() > since]

        self.logger.info(f"Filtered {len(filtered)} files modified after {since}")
        return filtered
