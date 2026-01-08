# FTP / SFTP / FTPS Extractor

Extract files from FTP servers supporting multiple protocols (FTP, SFTP, FTPS Explicit, FTPS Implicit) and save them to Keboola Storage.

## Key Features

- **Multiple Protocol Support**: Connect to FTP, SFTP, and FTPS servers
- **Flexible File Selection**: Use wildcard patterns, specific paths, or interactive file picker
- **Recursive Extraction**: Extract files from nested directories using `**` wildcards
- **Incremental Mode**: Extract only new or modified files since last run
- **SSH Key Authentication**: Support for key-based authentication for SFTP
- **Custom Tags**: Add tags to extracted files for organization
- **Output Control**: Flatten directory structure and/or append extraction timestamps

## Use Cases

- Extract daily reports from remote FTP servers
- Download log files from multiple directories
- Incrementally sync files from SFTP servers
- Retrieve data exports with custom naming patterns

## Configuration

1. Select your protocol (FTP, SFTP, or FTPS)
2. Enter connection credentials
3. Choose file selection method (wildcard, specific paths, or interactive)
4. Configure extraction options (flattening, timestamps, incremental mode)
5. Optionally add tags for file organization

All extracted files are automatically saved to Keboola Storage with properly generated manifests.
