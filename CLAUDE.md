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
- `pipedrive-cli backup [-o DIR] [-e ENTITY] [-n]` - Full backup as datapackage
- `pipedrive-cli store PATH [-n] [-e ENTITY] [-l FILE] [--no-update-base]` - Sync local data to Pipedrive
  - Alias: `restore` (deprecated name)
  - `--no-update-base`: Don't update local files with Pipedrive-assigned field keys
- `pipedrive-cli describe` - Show field schemas from API
- `pipedrive-cli validate PATH` - Validate existing backup
- `pipedrive-cli entities` - List available entities

### Field Management
All field commands support `--base PATH` for local operations or API operations (default).

- `pipedrive-cli field list -e ENTITY [-b PATH] [--custom-only]` - List fields
- `pipedrive-cli field copy -e ENTITY -f SOURCE -t TARGET [--transform TYPE] [-x] [-b PATH]` - Copy field values (`-x` exchanges display names)
- `pipedrive-cli field rename -e ENTITY -f FIELD -o NEW_NAME [-b PATH]` - Rename field display name
- `pipedrive-cli field delete -e ENTITY FIELD... [-b PATH] [--force]` - Delete custom field(s)

### Search & Filter
- `pipedrive-cli search -e ENTITY [-b PATH] [-f FILTER] [-i FIELDS] [-x FIELDS] [-o FORMAT] [-l LIMIT] [-n] [-q]`

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
- `contains(field, substr)` - Case-insensitive substring match
- `startswith(field, prefix)` - Case-insensitive prefix match
- `endswith(field, suffix)` - Case-insensitive suffix match
- `isnull(field)` - Check if null or empty
- `notnull(field)` - Check if not null
- `len(field)` - String length

**Operators:** `>`, `<`, `>=`, `<=`, `==`, `!=`, `and`, `or`, `not`

**Field Resolution:** Identifiers are resolved by key prefix, then name prefix (case-insensitive). Underscores are converted to spaces for name matching (`tel_s` → "Tel standard"). Error if ambiguous.

```bash
# Search with filter (shows resolved expression by default)
pipedrive-cli search -e per -f "contains(name, 'John')"
# Filter: contains(name, 'John')

# Dry-run to verify field resolution
pipedrive-cli search -e per -f "contains(First, 'test') and notnull(abc123)" -n
# Filter: contains(first_name, 'test') and notnull(abc123_custom_field)
# (dry-run: search not executed)

# JSON output (quiet mode for piping)
pipedrive-cli search -e deals -f "value > 10000" -o json -q

# Field selection
pipedrive-cli search -e per -i "id,name,email" --limit 10
```

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
