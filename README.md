# pipedrive-cli

CLI tool for backing up Pipedrive CRM data via API.

## Features
- Full account backup as Frictionless datapackage
- Auto-schema generation from Pipedrive custom fields
- Multiple export formats (CSV, JSON, Excel)
- Pagination and rate limiting handled automatically

## Installation
```bash
pip install -e .
```

## Usage
```bash
# Set API token
export PIPEDRIVE_API_TOKEN=your_token

# Full backup
pipedrive-cli backup --output ./backup/

# Describe schemas
pipedrive-cli describe

# Validate backup
pipedrive-cli validate ./backup/
```

## Output
Backups are stored as Frictionless datapackages:
- `datapackage.json` - Metadata and schemas
- CSV files for each entity type
- `files/` directory for attachments

## Tech Stack
- Python 3.11+
- Frictionless Framework (datapackage)
- httpx (async HTTP)
- Click (CLI)
