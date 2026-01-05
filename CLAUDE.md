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

## Commands (planned)
- `pipedrive-cli backup --output ./backup/` - Full backup as datapackage
- `pipedrive-cli describe` - Show inferred schemas from API
- `pipedrive-cli validate ./backup/` - Validate existing backup

## Development Principles
1. English language for all code/comments
2. Data-driven architecture (schemas from config, not code)
3. Git commit workflow: message in /tmp/commit-msg.txt
