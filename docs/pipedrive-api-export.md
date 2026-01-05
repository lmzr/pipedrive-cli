# Pipedrive API - Export Capabilities

## Authentication
- API token required (found in Settings > Personal preferences > API)
- Pass as query param: `?api_token=xxx` or header `Authorization: Bearer xxx`

## Main Endpoints for Export

| Entity | Endpoint | Pagination |
|--------|----------|------------|
| Persons | GET /v1/persons | limit (max 500), start |
| Organizations | GET /v1/organizations | limit, start |
| Deals | GET /v1/deals | limit, start |
| Activities | GET /v1/activities | limit, start |
| Notes | GET /v1/notes | limit, start |
| Products | GET /v1/products | limit, start |
| Files | GET /v1/files | limit, start |

## Schema Endpoints (for custom fields)
| Entity | Endpoint |
|--------|----------|
| Person fields | GET /v1/personFields |
| Organization fields | GET /v1/organizationFields |
| Deal fields | GET /v1/dealFields |
| Activity fields | GET /v1/activityFields |

## Pagination
- Default limit: 100
- Max limit: 500
- Use `start` param for offset
- Response includes `additional_data.pagination.more_items_in_collection`

## Rate Limiting
- 80 requests per 2 seconds per API token
- 429 Too Many Requests returned when exceeded
- Implement exponential backoff

## Response Format
```json
{
  "success": true,
  "data": [...],
  "additional_data": {
    "pagination": {
      "start": 0,
      "limit": 100,
      "more_items_in_collection": true,
      "next_start": 100
    }
  }
}
```

## Sources
- https://developers.pipedrive.com/docs/api/v1
- https://support.pipedrive.com/en/article/exporting-data-from-pipedrive
