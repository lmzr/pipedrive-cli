# pipedrive-cli

CLI tool for backing up and managing Pipedrive CRM data via API.

## Features
- Full account backup as Frictionless datapackage
- Restore/sync local data back to Pipedrive
- Field management (copy, rename, delete) on API or local data
- Auto-schema generation from Pipedrive custom fields
- Multiple export formats (CSV, JSON, Excel)
- Pagination and rate limiting handled automatically

## Installation
```bash
pip install -e .
```

## Usage

### Setup
```bash
# Set API token
export PIPEDRIVE_API_TOKEN=your_token
```

### Backup & Restore
```bash
# Full backup
pipedrive-cli backup --output ./backup/

# Backup specific entities
pipedrive-cli backup -o ./backup/ -e persons -e organizations

# Restore/sync local data to Pipedrive
pipedrive-cli store ./backup/

# Dry run (show what would happen)
pipedrive-cli store ./backup/ -n
```

### Field Management
```bash
# List fields for an entity
pipedrive-cli field list -e persons
pipedrive-cli field list -e per --custom-only  # Custom fields only

# Copy field values (with optional transformation)
pipedrive-cli field copy -e persons -f source_field -t target_field
pipedrive-cli field copy -e per -f phone -t mobile --transform varchar

# Copy and exchange display names
pipedrive-cli field copy -e per -f old_phone -t new_phone --exchange

# Rename field display name
pipedrive-cli field rename -e persons -f my_field -o "New Name"

# Delete custom field(s)
pipedrive-cli field delete -e persons my_custom_field
pipedrive-cli field delete -e per field1 field2 field3 --force
```

### Local Operations
All field commands support `--base PATH` to operate on local datapackage instead of API:

```bash
# Work on local backup
pipedrive-cli field list -e persons --base ./backup/
pipedrive-cli field copy -e per -f source -t "New Field" --base ./backup/
pipedrive-cli field delete -e per old_field --base ./backup/ --force
```

### Search & Filter
```bash
# Search persons with filter
pipedrive-cli search -e persons -f "contains(name, 'John')"

# Search with numeric comparison
pipedrive-cli search -e deals -f "int(value) > 10000" -o json

# Search in local backup
pipedrive-cli search -e per --base ./backup/ -f "notnull(email)"
```

### Update Values
```bash
# Update field values matching a filter
pipedrive-cli value update -e persons -f "isnull(phone)" -s "phone='N/A'"

# Pad codes to 5 digits
pipedrive-cli value update -e deals -b ./backup/ \
  -f "isint(code)" \
  -s "code=lpad(code, 5, '0')"
```

See [Filter and Expression Reference](docs/expressions.md) for complete documentation on filter syntax, functions, and examples.

### Other Commands
```bash
# Describe field schemas from API
pipedrive-cli describe

# Validate backup integrity
pipedrive-cli validate ./backup/

# List available entities
pipedrive-cli entities
```

## Output Format
Backups are stored as Frictionless datapackages:
```
backup-2026-01-05/
├── datapackage.json       # Metadata + schemas
├── persons.csv
├── organizations.csv
├── deals.csv
├── activities.csv
├── notes.csv
└── files/                 # Binary files
```

## Tech Stack
- Python 3.11+
- Frictionless Framework (datapackage)
- httpx (async HTTP)
- Click (CLI)
- Rich (terminal UI)
