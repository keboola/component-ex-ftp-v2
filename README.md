# FTP / SFTP / FTPS Extractor v2

Extract files from FTP, SFTP, and FTPS servers to Keboola Storage.

## Description

This component allows you to extract files from FTP servers supporting multiple protocols (FTP, SFTP, FTPS Explicit, and FTPS Implicit) and save them to Keboola Storage. It supports flexible file selection methods including wildcard patterns, specific file paths, and interactive file selection.

## Features

| **Feature**                | **Description**                                                          |
|----------------------------|--------------------------------------------------------------------------|
| **Multiple Protocols**     | Supports FTP, SFTP, FTPS (Explicit), and FTPS (Implicit)               |
| **Flexible File Selection**| Select files by wildcard pattern, specific paths, or interactive picker |
| **Recursive Matching**     | Extract files from nested directory structures using `**` wildcards     |
| **Incremental Extraction** | Optional mode to extract only new/modified files since last run         |
| **File Manifests**         | Automatically generates Keboola file manifests with custom tags         |
| **Row-Based Configuration**| Configure multiple extraction jobs with different settings               |
| **Connection Testing**     | Test connection button to verify credentials before running              |
| **SSH Key Authentication** | Support for SSH key-based authentication for SFTP                        |
| **Output Options**         | Flatten directory structure and/or append extraction timestamp           |

## Configuration

### Connection Settings

#### Protocol
Select the protocol to use:
- **SFTP** (default) - Secure File Transfer Protocol over SSH
- **FTP** - File Transfer Protocol
- **FTPS (Explicit)** - FTP with explicit TLS/SSL
- **FTPS (Implicit)** - FTP with implicit TLS/SSL

#### Host URL
The hostname or IP address of the FTP server.

#### Port
Port number for the server connection. Defaults:
- SFTP: 22
- FTP/FTPS Explicit: 21
- FTPS Implicit: 990

#### Username & Password
Credentials for authentication. Password is stored securely.

### SFTP-Specific Settings

#### SSH Keys
For SFTP connections, you can optionally provide SSH private keys for authentication. Supports RSA, ECDSA, and Ed25519 keys.

#### Passphrase
Optional passphrase for encrypted SSH keys.

#### Disabled Algorithms
JSON object to disable specific SSH algorithms. Example:
```json
{"pubkeys": ["rsa-sha2-256", "rsa-sha2-512"]}
```

#### Banner Timeout
Timeout in seconds to wait for SSH banner (default: 120). Increase for slow servers.

### FTP/FTPS Settings

#### Passive Mode
Enable passive mode for FTP/FTPS connections (default: enabled). Recommended for connections through firewalls.

### File Selection

#### File Selection Mode
Choose how to select files for extraction:

1. **Wildcard Pattern** - Use glob-style patterns to match files
2. **Specific Paths** - Provide exact file paths (one per line)
3. **Select from List** - Interactively select files from the server

#### Wildcard Pattern Examples

Simple wildcard (single directory):
```
/data/*.csv
```

Recursive wildcard (all subdirectories):
```
/logs/**/*.txt
/data/**/report_*.json
```

Multiple patterns (in path mode):
```
/data/file1.csv
/data/file2.csv
/logs/app.log
```

### Extraction Options

#### Flatten Directory Structure
When enabled, all files are saved in a flat structure with path separators replaced by underscores.

Example:
- Input: `/data/subfolder/file.csv`
- Output: `data_subfolder_file.csv`

#### Append Extraction Timestamp
When enabled, adds extraction timestamp to filenames.

Example:
- Input: `file.csv`
- Output: `file_20240108123456.csv`

#### Incremental Extraction
When enabled, only extracts files that are new or modified since the last extraction. Uses file modification time tracking via state file.

#### File Tags
Optional comma-separated tags to add to extracted files in Keboola Storage.

### Advanced Settings

#### Connection Timeout
Timeout in seconds for establishing server connection (default: 30).

#### Maximum Retries
Number of retry attempts for failed operations (default: 2).

#### Debug Mode
Enable detailed logging for troubleshooting.

## Output

All extracted files are saved to Keboola Storage with the following structure:

- Files are written to `out/files/` directory
- Each file gets a `.manifest` file with metadata (tags, permanence settings)
- Directory structure is preserved by default (unless flattening is enabled)

### File Manifests

Each extracted file includes a manifest file with:
- `is_permanent`: true (files are kept in Storage)
- `tags`: User-specified tags (if provided)
- `is_public`: false
- `is_encrypted`: false

## Examples

### Example 1: Extract CSV files from FTP server

```json
{
  "protocol": "FTP",
  "hostname": "ftp.example.com",
  "port": 21,
  "user": "username",
  "#pass": "password",
  "passive_mode": true,
  "file_selection_mode": "wildcard",
  "file_pattern": "/exports/*.csv",
  "flatten_output": false,
  "tags": ["daily-export", "csv"]
}
```

### Example 2: Recursive SFTP extraction with SSH key

```json
{
  "protocol": "SFTP",
  "hostname": "sftp.example.com",
  "port": 22,
  "user": "sftpuser",
  "#pass": "",
  "ssh": {
    "keys": {
      "#private": "-----BEGIN RSA PRIVATE KEY-----\n..."
    }
  },
  "file_selection_mode": "wildcard",
  "file_pattern": "/data/**/*.json",
  "flatten_output": true,
  "append_timestamp": true,
  "tags": ["json-data"]
}
```

### Example 3: Incremental extraction

```json
{
  "protocol": "SFTP",
  "hostname": "sftp.example.com",
  "port": 22,
  "user": "user",
  "#pass": "password",
  "file_selection_mode": "wildcard",
  "file_pattern": "/incoming/*.csv",
  "incremental_mode": true,
  "tags": ["incremental"]
}
```

## Development

### Prerequisites

- Python 3.13+
- Docker and Docker Compose (for testing)
- uv (Python package manager)

### Setup

1. Clone the repository
2. Install dependencies:
   ```bash
   uv pip install -e .
   ```

### Running Tests

Run the test suite with Docker Compose:

```bash
docker-compose run --rm test
```

This will:
1. Start FTP and SFTP test servers
2. Run flake8 linting
3. Run unit tests
4. Run E2E tests against real FTP/SFTP servers

### Running Locally

```bash
docker-compose run --rm dev
```

### Project Structure

```
src/
├── component.py           # Main component logic
├── configuration.py       # Pydantic configuration models
├── ftp_client.py         # FTP/SFTP/FTPS client implementations
├── file_matcher.py       # Pattern matching logic
└── manifest_writer.py    # Manifest file generation

tests/
├── test_component.py               # Unit and E2E tests
├── expected_server_data/           # Test files for FTP/SFTP servers
│   ├── ftp/                       # FTP test files
│   └── sftp/                      # SFTP test files
└── e2e_configs/                   # E2E test configurations
    ├── ftp_wildcard/
    └── sftp_wildcard/

component_config/
├── configSchema.json              # UI configuration schema
├── configRowSchema.json           # Row-level configuration schema
└── component_short_description.md
```

## Troubleshooting

### Connection Issues

1. **FTP Connection Timeout**
   - Increase `connection_timeout` setting
   - Check firewall rules
   - Verify passive mode setting for FTP/FTPS

2. **SFTP Authentication Failed**
   - Verify username and password
   - Check SSH key format if using key authentication
   - Increase `banner_timeout` for slow servers
   - Try disabling problematic SSH algorithms

3. **No Files Found**
   - Verify file pattern syntax
   - Check file permissions on the server
   - Use debug mode to see detailed file listing logs
   - Test with absolute paths starting with `/`

### Pattern Matching

- Use `/` for paths (even on Windows servers)
- Use `*` for matching within a single directory
- Use `**` for recursive matching across subdirectories
- Patterns are case-sensitive

## License

MIT

## Support

For issues, feature requests, or questions:
- Submit issues on GitHub
- Contact Keboola support
- Visit [ideas.keboola.com](https://ideas.keboola.com/) for feature requests
