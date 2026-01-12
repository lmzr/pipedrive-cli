# CLAUDE.md - pipedrive-cli

## Project Overview
CLI tool for backing up and exporting Pipedrive CRM data via API.

## Key Principle: Data-Driven with Frictionless
**Use Frictionless datapackage for all data schema definitions - NO hardcoded formats.**

Frictionless provides:
- **Describe**: Infer metadata/schema from Pipedrive API responses
- **Extract**: Read tabular data with unified interface
- **Validate**: Ensure data quality
- **Transform**: Export to multiple formats (CSV, JSON, Excel)

## Objectives
1. Export all Pipedrive data types as a datapackage
2. Auto-generate schemas from Pipedrive custom fields
3. Handle pagination and rate limiting
4. Support incremental backups (optional)

## Architecture
- Data schemas defined via Frictionless Table Schema (JSON)
- No hardcoded field mappings - schemas derived from API
- One resource per entity type in the datapackage

## Tech Stack
- Python 3.11+
- Click (CLI)
- httpx (async HTTP client)
- frictionless (datapackage, schema inference, export)
- rich (terminal UI)
- simpleeval (filter expressions)

## Pipedrive API Documentation

**Official docs:**
- API Reference: https://developers.pipedrive.com/docs/api/v1
- Tutorials: https://developers.pipedrive.com/tutorials
- Community: https://devcommunity.pipedrive.com/

**API Version:**
- This project uses **API v1** (`/v1/` prefix on all endpoints)
- Pipedrive is progressively releasing API v2 for some endpoints
- v2 changes: different response structure, cursor-based pagination
- We stay on v1 for stability and full endpoint coverage

**Authentication:**
- Use `api_token` as **query parameter**: `?api_token=xxx`
- Do NOT use Authorization header (not supported for API tokens)
- Token found in: Settings > Personal preferences > API

**Rate Limiting:**
- 80 requests per 2 seconds per API token
- HTTP 429 returned when exceeded
- Automatic retry with Retry-After header

**Error Handling:**
- Custom exceptions in `exceptions.py`: `PipedriveError`, `AuthenticationError`, `NotFoundError`, etc.
- Automatic retry (3x) with exponential backoff for 5xx errors
- API error messages parsed from `result.error` field

**Pagination:**
- Default limit: 100, max: 500
- Use `start` param for offset
- Check `additional_data.pagination.more_items_in_collection`

**CRUD Operations:**
- GET /v1/{entity} - List all
- GET /v1/{entity}/{id} - Get one
- POST /v1/{entity} - Create
- PUT /v1/{entity}/{id} - Update
- DELETE /v1/{entity}/{id} - Delete

## Pipedrive API Endpoints
| Entity | Endpoint | Notes |
|--------|----------|-------|
| Persons | GET /persons | Includes custom fields |
| Organizations | GET /organizations | |
| Deals | GET /deals | |
| Activities | GET /activities | |
| Notes | GET /notes | |
| Products | GET /products | |
| Files | GET /files | Binary download separate |
| PersonFields | GET /personFields | Schema for persons |
| OrganizationFields | GET /organizationFields | Schema for orgs |
| DealFields | GET /dealFields | Schema for deals |

## Output Format (datapackage)
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

## Commands

### Backup & Restore
- `pipedrive-cli backup [-o DIR] [-e ENTITY] [-n] [--limit N]` - Full backup as datapackage
  - `--limit N`: Maximum records per entity (for testing)
- `pipedrive-cli store PATH [-n] [-e ENTITY] [-l FILE] [--no-update-base] [--resume] [--skip-unchanged] [--limit N]` - Sync local data to Pipedrive
  - Alias: `restore` (deprecated name)
  - `--no-update-base`: Don't update local files with Pipedrive-assigned IDs
  - `--resume`: Continue from partial sync using existing `id_mapping.jsonl`
  - `--skip-unchanged`: Skip records that haven't changed (compares with Pipedrive data)
  - `--limit N`: Maximum records per entity (for testing)
  - **ID remapping**: Reference fields (org_id, person_id, etc.) are automatically remapped to Pipedrive-assigned IDs
  - **Field sync**: Field display names are synchronized (renamed fields are updated in Pipedrive)
- `pipedrive-cli describe` - Show field schemas from API
- `pipedrive-cli validate PATH` - Validate existing backup
- `pipedrive-cli entities` - List available entities

### Field Management
All field commands support `--base PATH` for local operations or API operations (default).

- `pipedrive-cli field list -e ENTITY [-b PATH] [--custom-only]` - List fields
- `pipedrive-cli field create -e ENTITY -b PATH NAME -t TYPE [-o OPTIONS...] [-n]` - Create custom field (local only)
- `pipedrive-cli field copy -e ENTITY -f SOURCE -t TARGET [--transform TYPE] [-x] [-b PATH]` - Copy field values (`-x` exchanges display names)
- `pipedrive-cli field rename -e ENTITY -f FIELD -o NEW_NAME [-b PATH]` - Rename field display name
- `pipedrive-cli field delete -e ENTITY FIELD... [-b PATH] [--force]` - Delete custom field(s)

### Field Options (enum/set)
Manage options for enum and set type fields.

- `pipedrive-cli field options list -e ENTITY -f FIELD [-b PATH] [--show-usage]` - List options
- `pipedrive-cli field options add -e ENTITY -b PATH -f FIELD [-n] VALUES...` - Add options
- `pipedrive-cli field options remove -e ENTITY -b PATH -f FIELD [--force] [-n] VALUES...` - Remove options
- `pipedrive-cli field options sync -e ENTITY -b PATH -f FIELD [-n]` - Sync options with data values

### Data Conversion
- `pipedrive-cli data convert INPUT -o OUTPUT [-s SHEET] [-r ROW] [--preserve-links]`

| Option | Description |
|--------|-------------|
| `INPUT` | Source XLSX file |
| `-o, --output` | Output file path (CSV or JSON) |
| `-s, --sheet` | Sheet name (default: first) |
| `-r, --header-row` | Header row number (default: 1) |
| `--preserve-links` | Extract hyperlink URLs instead of display text |

**Note:** XLSX support requires openpyxl: `pip install pipedrive-cli[xlsx]`

### Record Operations
- `pipedrive-cli record search -e ENTITY [-b PATH] [-f FILTER] [-i FIELDS] [-x FIELDS] [-o FORMAT] [-l LIMIT] [-n] [-q]`
- `pipedrive-cli record update -e ENTITY [-b PATH] [-f FILTER] -s ASSIGNMENT... [-n] [-q] [-l FILE] [--limit N]`
- `pipedrive-cli record import -e ENTITY -b PATH -i FILE [-k KEY] [--on-duplicate update|skip|error] [--auto-id] [-s SHEET] [-n] [-l LOG] [-q]`
- `pipedrive-cli record delete -e ENTITY [-b PATH] [-f FILTER] [-n] [-q] [-l LOG] [--limit N] [--force]` (see README)

**Alias:** `pipedrive-cli search` → `pipedrive-cli record search` (for backward compatibility)

#### Search Options

| Option | Description |
|--------|-------------|
| `-e, --entity` | Entity type (supports prefix matching) |
| `-b, --base` | Search local datapackage instead of API |
| `-f, --filter` | Filter expression using simpleeval |
| `-i, --include` | Comma-separated field prefixes to include |
| `-x, --exclude` | Comma-separated field prefixes to exclude |
| `-o, --format` | Output format: `table` (default), `json`, `csv` |
| `-l, --limit` | Maximum number of records |
| `-n, --dry-run` | Show resolved filter only (no search) |
| `-q, --quiet` | Don't show resolved filter before results |

**Filter Functions:**
- `field("name")` - Exact field name lookup (supports accents, spaces, special chars)
- `contains(field, substr)` - Case-insensitive substring match
- `startswith(field, prefix)` - Case-insensitive prefix match
- `endswith(field, suffix)` - Case-insensitive suffix match
- `isnull(field)` - Check if null or empty
- `notnull(field)` - Check if not null
- `len(field)` - String length
- `isint(field)` - True if text is a valid integer
- `isfloat(field)` - True if text is a valid float
- `isnumeric(field)` - True if text is numeric (int or float)
- `substr(field, start, end)` - Substring extraction
- `replace(field, old, new)` - String replacement
- `strip(field)`, `lstrip(field)`, `rstrip(field)` - Whitespace removal
- `lpad(field, width, char)` - Left pad
- `rpad(field, width, char)` - Right pad
- `upper(field)`, `lower(field)` - Case conversion
- `concat(a, b, ...)` - String concatenation
- `int(field)`, `float(field)`, `str(field)` - Type conversion

**Operators:** `>`, `<`, `>=`, `<=`, `==`, `!=`, `and`, `or`, `not`

**Type conversion:** No automatic coercion. Use explicit `int()`, `float()`, `str()` for type conversion (e.g., `int(age) > 25`).

**Field Resolution:** Identifiers are resolved by key prefix, then name prefix (case-insensitive). Underscores are converted to spaces for name matching (`tel_s` → "Tel standard"). Error if ambiguous.

**Digit-starting keys:** Pipedrive field keys are SHA-1 hashes that may start with digits (e.g., `25da23b...`). These are handled automatically:
- Hex-like prefixes containing letters a-f are detected: `25da` → `_25da23b...`
- Pure numbers like `25` are NOT resolved (treated as numeric literals)
- Users can explicitly escape with `_` prefix: `_25` → matches key `25da...`

```bash
# Search with filter (shows resolved expression by default)
pipedrive-cli record search -e per -f "contains(name, 'John')"
# Filter: contains(name, 'John')

# Numeric comparison (explicit int conversion)
pipedrive-cli record search -e deals -f "int(value) > 10000" -o json -q

# Dry-run to verify field resolution
pipedrive-cli record search -e per -f "contains(First, 'test') and notnull(abc123)" -n
# Filter: contains(first_name, 'test') and notnull(abc123_custom_field)
# (dry-run: search not executed)

# Digit-starting key prefix (auto-detected with hex letters)
pipedrive-cli record search -e per -f "25da != b85f"
# Filter: "Civilité-OLD" != Civilité

# Digit-starting key with explicit _ prefix (for pure numeric prefixes)
pipedrive-cli record search -e per -f "notnull(_25)"
# Filter: notnull("Civilité-OLD")

# Exact field name with accents or special characters
pipedrive-cli record search -e per -f 'notnull(field("Civilité"))'
# Filter: notnull(Civilité)

# Field selection
pipedrive-cli record search -e per -i "id,name,email" --limit 10
```

#### Update Options

| Option | Description |
|--------|-------------|
| `-e, --entity` | Entity type (supports prefix matching) |
| `-b, --base` | Update local datapackage instead of API |
| `-f, --filter` | Filter expression to select records |
| `-s, --set` | Field assignment `field=expr` (can be repeated) |
| `-n, --dry-run` | Preview changes without applying |
| `-q, --quiet` | Don't show resolved expressions |
| `-l, --log` | Write detailed log to file (JSON lines) |
| `--limit` | Maximum records to update |

**Transform Functions:**
- `field("name")` - Exact field name lookup (supports accents, spaces, special chars)
- `upper(s)`, `lower(s)` - Case conversion
- `strip(s)`, `lstrip(s)`, `rstrip(s)` - Whitespace removal
- `replace(s, old, new)` - String replacement
- `lpad(s, width, char)` - Left pad: `lpad('7', 5, '0')` → `'00007'`
- `rpad(s, width, char)` - Right pad: `rpad('7', 5, '0')` → `'70000'`
- `substr(s, start, end)` - Substring extraction
- `concat(a, b, ...)` - String concatenation (or use `+`)
- `int(s)`, `float(s)`, `str(n)` - Type conversion
- `round(n, d)`, `abs(n)` - Numeric operations
- `iif(cond, then, else)` - Conditional (use `iif` not `if`)
- `coalesce(a, b, ...)` - First non-null value

**Operators:** `+`, `-`, `*`, `/`, `%`, `and`, `or`, `not`

```bash
# Prepend '0' to phone numbers without dots
pipedrive-cli record update -e per -b backup/ \
  -f "not(contains(tel_s, '.'))" \
  -s "tel_s='0' + tel_s"
# Output:
# Filter w/ names: not(contains("Tel standard", '.'))
# Filter w/ keys:  not(contains(_new_bf8adae, '.'))
# Set w/ names:    "Tel standard" = '0' + "Tel standard"
# Set w/ keys:     _new_bf8adae = '0' + _new_bf8adae

# Uppercase names
pipedrive-cli record update -e per -s "name=upper(name)"

# Pad codes to 5 digits
pipedrive-cli record update -e deals -f "notnull(code)" -s "code=lpad(code, 5, '0')"

# Multiple assignments
pipedrive-cli record update -e per \
  -s "first_name=upper(first_name)" \
  -s "last_name=upper(last_name)"

# Filter on numeric text fields
pipedrive-cli record update -e per -f "isint(code)" -s "code=lpad(code, 5, '0')"

# Dry-run with log
pipedrive-cli record update -e per -b data/ -f "..." -s "..." -n -l changes.jsonl
```

#### Import Options

| Option | Description |
|--------|-------------|
| `-e, --entity` | Entity type (supports prefix matching) |
| `-b, --base` | Target datapackage directory |
| `-i, --input` | Input file (CSV, JSON, or XLSX) |
| `-k, --key` | Field(s) for deduplication (comma-separated) |
| `--on-duplicate` | Action on duplicate: `update` (default), `skip`, `error` |
| `--auto-id` | Generate IDs for new records |
| `-s, --sheet` | Sheet name for XLSX files |
| `-n, --dry-run` | Preview without changes |
| `-l, --log` | Write detailed log (JSON lines) |
| `-q, --quiet` | Suppress verbose output |

### Local Field Workflow

When creating fields locally (with `--base`), a unique local ID is generated:

```bash
# 1. Create new field locally
pipedrive-cli field copy -e per -b data/ -f source_field -t "My New Field" --transform varchar
# → Creates: key="_new_abc1234", name="My New Field"

# 2. Rename display name (key unchanged)
pipedrive-cli field rename -e per -b data/ -f "My New Field" -o "Better Name"
# → Updates: key="_new_abc1234", name="Better Name"

# 3. Sync to Pipedrive
pipedrive-cli store data/
# → Pipedrive assigns real key: "hash123_better_name"
# → Local datapackage and CSV updated automatically
```

**Local field keys** start with `_new_` prefix and are replaced by Pipedrive-assigned keys during `store`.

**Schema synchronization**: All local field operations (`copy`, `delete`) update both:
- `pipedrive_fields` (Pipedrive field definitions in schema.custom)
- `schema.fields` (Frictionless table schema)
- CSV columns

## Development Principles
1. English language for all code/comments
2. Data-driven architecture (schemas from config, not code)
3. Git commit workflow: message in /tmp/commit-msg.txt
