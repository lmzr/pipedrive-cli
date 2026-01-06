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

## Pipedrive API Documentation

**Official docs:**
- API Reference: https://developers.pipedrive.com/docs/api/v1
- Tutorials: https://developers.pipedrive.com/tutorials
- Community: https://devcommunity.pipedrive.com/

**Authentication:**
- Use `api_token` as **query parameter**: `?api_token=xxx`
- Do NOT use Authorization header (not supported for API tokens)
- Token found in: Settings > Personal preferences > API

**Rate Limiting:**
- 80 requests per 2 seconds per API token
- HTTP 429 returned when exceeded
- Use exponential backoff with Retry-After header

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
- `pipedrive-cli backup [-o DIR] [-e ENTITY]` - Full backup as datapackage
- `pipedrive-cli restore PATH [-n] [-e ENTITY] [-l FILE]` - Restore backup to Pipedrive
- `pipedrive-cli describe` - Show field schemas from API
- `pipedrive-cli validate PATH` - Validate existing backup
- `pipedrive-cli entities` - List available entities

## Development Principles
1. English language for all code/comments
2. Data-driven architecture (schemas from config, not code)
3. Git commit workflow: message in /tmp/commit-msg.txt
