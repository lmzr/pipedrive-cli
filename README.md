# pipedrive-cli

CLI tool for backing up and managing Pipedrive CRM data via API.

## Features
- Full account backup as Frictionless datapackage
- Restore/sync local data back to Pipedrive
- Import records from CSV/JSON/XLSX into local datapackage
- Field management (create, copy, rename, delete) on API or local data
- Manage enum/set field options (list, add, remove, sync)
- Convert XLSX to CSV/JSON with hyperlink preservation
- Auto-schema generation from Pipedrive custom fields
- Multiple export formats (CSV, JSON, Excel)
- Pagination and rate limiting handled automatically

## Installation
```bash
pip install -e .

# With XLSX support (for data convert and record import)
pip install -e ".[xlsx]"
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

# Resume from partial sync (after failure)
pipedrive-cli store ./backup/ --resume

# Skip unchanged records (compare with Pipedrive before updating)
pipedrive-cli store ./backup/ --skip-unchanged
```

**Store features:**
- **ID remapping**: Reference fields (org_id, person_id, etc.) are automatically remapped to Pipedrive-assigned IDs
- **Local update**: After store, local CSV files are updated with new Pipedrive IDs
- **Field sync**: Field display names are synchronized (renamed fields are updated in Pipedrive)
- **Resume**: Use `--resume` to continue from `id_mapping.jsonl` after a partial sync failure
- **Skip unchanged**: Use `--skip-unchanged` to only update records that have actually changed

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

# Create custom field (local only)
pipedrive-cli field create -e persons -b backup/ "Category" -t enum \
  -o "LEADER" -o "POWER USER" -o "PROSPECT"

# Manage field options
pipedrive-cli field options list -e per -b backup/ -f category --show-usage
pipedrive-cli field options add -e per -b backup/ -f category "New Type"
pipedrive-cli field options remove -e per -b backup/ -f category "Old Type"
pipedrive-cli field options sync -e per -b backup/ -f category
```

### Record Import
```bash
# Import CSV with auto-ID generation
pipedrive-cli record import -e persons -b backup/ -i contacts.csv --auto-id

# Import with deduplication by email
pipedrive-cli record import -e persons -b backup/ -i new_data.csv \
  -k email --on-duplicate update

# Import XLSX (requires openpyxl)
pipedrive-cli record import -e deals -b backup/ -i sales.xlsx -s "Q4 Data"
```

### Data Conversion
```bash
# Convert XLSX to CSV
pipedrive-cli data convert contacts.xlsx -o contacts.csv

# Extract hyperlinks from XLSX
pipedrive-cli data convert links.xlsx -o links.csv --preserve-links

# Specify sheet and header row
pipedrive-cli data convert report.xlsx -o report.csv -s "Data" -r 3
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
pipedrive-cli record search -e persons -f "contains(name, 'John')"

# Search with numeric comparison
pipedrive-cli record search -e deals -f "int(value) > 10000" -o json

# Search in local backup
pipedrive-cli record search -e per --base ./backup/ -f "notnull(email)"
```

### Update Values
```bash
# Update field values matching a filter
pipedrive-cli record update -e persons -f "isnull(phone)" -s "phone='N/A'"

# Pad codes to 5 digits
pipedrive-cli record update -e deals -b ./backup/ \
  -f "isint(code)" \
  -s "code=lpad(code, 5, '0')"
```

### Delete Records
```bash
# Preview deletion (dry-run)
pipedrive-cli record delete -e persons -b ./backup/ -f "contains(name, 'TEST')" -n

# Delete with confirmation prompt
pipedrive-cli record delete -e persons -b ./backup/ -f "isnull(email)"

# Force delete without confirmation
pipedrive-cli record delete -e deals -f "value == 0" --force
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
