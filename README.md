markdown
# Redis-Key-Management-Toolkit

This repository contains a set of Python scripts designed to manage Redis keys efficiently. It includes tools for both exporting Redis data and deleting keys with a backup mechanism. These scripts are ideal for handling large sets of keys and ensuring data integrity in batch operations.

## Contents

1. **Redis Data Exporter** - A script to export data from Redis to CSV format.
2. **Redis Key Deleter with Backup** - A script for safely deleting keys from Redis while backing up their values.

## Redis Data Exporter

### Features

- Batch Processing
- Threaded Fetching
- Redis Sentinel Support
- Progress Tracking

### Usage

```bash
python redis-export.py <keys_file_path> [redis_host] [redis_port] [redis_db] [sentinel_host] [sentinel_port] [master_name] [redis_password]
```

### Output

Generates a CSV file named `<keys_file_path>-<timestamp>.csv`, containing the key-value pairs from Redis.

## Redis Key Deleter with Backup

### Features

- Efficient Batch Deletion
- Multi-Threaded Operations
- Handles Derived Keys
- Backs Up Deleted Key Values

### Usage

```bash
python redis-delete.py <keys_file_path> [redis_host] [redis_port] [redis_db] [sentinel_host] [sentinel_port] [master_name] [redis_password]
```

### Output

Creates a text file named `<keys_file_path>-deleted-<timestamp>.txt`, containing the backed-up key-value pairs.

## Prerequisites

- Python 3.x
- Access to a Redis database or Redis Sentinel setup
- `redis` Python package (`pip install redis`)

## Installation

Clone the repository and install the required dependencies.

```bash
git clone [Repository URL]
cd Redis-Key-Management-Toolkit
pip install -r requirements.txt
```

## License

[Specify License Here]

## Contributing

Contributions, issues, and feature requests are welcome. Feel free to check [issues page] for open issues or submit new ones.

## Authors

- [lamlesh1988](https://github.com/kamlesh1988)
## Disclaimer

These scripts are provided 'as is', and the author is not responsible for any consequences resulting from their use. Users are advised to test them in a development environment before deploying to production.

---
