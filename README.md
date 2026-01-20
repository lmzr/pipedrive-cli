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

# Backup specific entities (comma-separated or repeated)
pipedrive-cli backup -o ./backup/ -e persons,organizations
pipedrive-cli backup -o ./backup/ -e persons -e organizations

# Restore/sync local data to Pipedrive
pipedrive-cli store ./backup/

# Dry run (show what would happen)
pipedrive-cli store ./backup/ -n

# Resume from partial sync (after failure)
pipedrive-cli store ./backup/ --resume

# Skip unchanged records (compare with Pipedrive before updating)
pipedrive-cli store ./backup/ --skip-unchanged

# Limit records per entity (for testing)
pipedrive-cli backup -o ./test/ -e organizations --limit 10
pipedrive-cli store ./test/ --skip-unchanged --dry-run --limit 10
```

**Store features:**
- **ID remapping**: Reference fields (org_id, person_id, etc.) are automatically remapped to Pipedrive-assigned IDs
- **Local update**: After store, local CSV files are updated with new Pipedrive IDs
- **Field sync**: Field display names are synchronized (renamed fields are updated in Pipedrive)
- **Resume**: Use `--resume` to continue from `id_mapping.jsonl` after a partial sync failure
- **Skip unchanged**: Use `--skip-unchanged` to only update records that have actually changed

### Field Management
```bash
# List fields for an entity (shows Pipedrive and Frictionless types)
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

# Import XLSX with headers on row 2
pipedrive-cli record import -e per -b backup/ -i data.xlsx -s "Sheet1" -r 2
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

# Search with numeric comparison (auto-coerced for local backups)
pipedrive-cli record search -e deals --base ./backup/ -f "value > 10000" -o json

# Search by ID (no quotes needed for local backups)
pipedrive-cli record search -e org --base ./backup/ -f "id == 462"

# Search in local backup
pipedrive-cli record search -e per --base ./backup/ -f "notnull(email)"
```

**Note:** When searching local datapackages (`--base`), field values are automatically converted to their schema types (integer, number, boolean). This means `id == 1` and `value > 1000` work directly without needing `int(id) == 1` or `id == "1"`.

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

### Find Duplicates
```bash
# Find email duplicates
pipedrive-cli record duplicates -e persons -k email

# Composite key (first_name + last_name)
pipedrive-cli record duplicates -e per -k "first_name,last_name"

# With filter to narrow scope
pipedrive-cli record duplicates -e per -k email -f "notnull(email)"

# JSON output for scripting
pipedrive-cli record duplicates -e org -k name -o json -q

# Summary statistics only
pipedrive-cli record duplicates -e per -k email --summary-only

# Include records with null key values
pipedrive-cli record duplicates -e per -k phone --include-nulls
```

| Option | Description |
|--------|-------------|
| `-e, --entity` | Entity type (supports prefix matching) |
| `-k, --key` | Field(s) for duplicate detection (comma-separated, required) |
| `-b, --base` | Search local datapackage instead of API |
| `-f, --filter` | Filter expression to narrow scope before detection |
| `-i, --include` | Field prefixes to include in output |
| `-x, --exclude` | Field prefixes to exclude from output |
| `-o, --format` | Output format: `table` (default), `json`, `csv` |
| `-l, --limit` | Maximum duplicate groups to show |
| `--include-nulls` | Include records with null key values (excluded by default) |
| `--summary-only` | Show statistics only without record details |
| `-n, --dry-run` | Show resolved key/filter only |
| `-q, --quiet` | Don't show resolved expressions |

See [Filter and Expression Reference](docs/expressions.md) for complete documentation on filter syntax, functions, and examples.

### Compare Datapackages
```bash
# Compare two backups (all entities)
pipedrive-cli diff backup-before backup-after

# Schema changes only
pipedrive-cli diff old/ new/ --schema-only

# Data changes only
pipedrive-cli diff old/ new/ --data-only

# Filter specific entities (comma-separated or repeated)
pipedrive-cli diff old/ new/ -e persons,deals
pipedrive-cli diff old/ new/ -e persons -e deals

# Custom matching key (global)
pipedrive-cli diff old/ new/ -k name

# Custom matching key (per-entity)
pipedrive-cli diff old/ new/ -k persons:email -k deals:title

# JSON output for CI/CD
pipedrive-cli diff before/ after/ -o json --exit-code

# Limit displayed record changes
pipedrive-cli diff old/ new/ --limit 10

# Include computed fields (timestamps, counters)
pipedrive-cli diff old/ new/ --all-fields
```

| Option | Description |
|--------|-------------|
| `PATH1` | First datapackage (source/before) |
| `PATH2` | Second datapackage (target/after) |
| `-e, --entity` | Filter to specific entity (supports prefix matching) |
| `--schema-only` | Compare only field definitions |
| `--data-only` | Compare only records |
| `-k, --key` | Matching key: `field` (global) or `entity:field` (per-entity). Default: `id` |
| `--all-fields` | Compare all fields including computed ones (excluded by default) |
| `-o, --format` | Output format: `table` (default), `json` |
| `--limit` | Maximum changed records to display per entity |
| `-q, --quiet` | Suppress headers and summaries |
| `--exit-code` | Return exit code 1 if differences found (for CI/CD) |

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
